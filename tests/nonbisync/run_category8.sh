#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_ROOT="${TMPDIR:-/tmp}/redisgunyu-nonbisync-cat8"
source "${ROOT}/tests/nonbisync/lib/test_env.sh"

SCENARIOS="${SCENARIOS:-sync,pipeline}"
RICH_KEY_SETS="${RICH_KEY_SETS:-64}"
SYNC_SRC_BASE="${SYNC_SRC_BASE:-34700}"
SYNC_DST_BASE="${SYNC_DST_BASE:-34800}"
SYNC_HTTP_PORT="${SYNC_HTTP_PORT:-34780}"
PIPE_SRC_BASE="${PIPE_SRC_BASE:-34900}"
PIPE_DST_BASE="${PIPE_DST_BASE:-35000}"
PIPE_HTTP_PORT="${PIPE_HTTP_PORT:-34980}"
TEST_PREFIX="${TEST_PREFIX:-nonbisync:cat8:$(date +%s)}"
SYNCER_PID=""
REDIS_SERVER_BIN="$(resolve_redis_server_bin REDIS_SERVER_BIN REDIS_DEPLOY_ROOT)"

cleanup() {
  local code=$?
  set +e
  stop_pid "${SYNCER_PID:-}"
  shutdown_ports \
    "${SYNC_SRC_BASE}" "$((SYNC_SRC_BASE + 1))" "$((SYNC_SRC_BASE + 2))" \
    "${SYNC_DST_BASE}" "$((SYNC_DST_BASE + 1))" "$((SYNC_DST_BASE + 2))" \
    "${PIPE_SRC_BASE}" "$((PIPE_SRC_BASE + 1))" "$((PIPE_SRC_BASE + 2))" \
    "${PIPE_DST_BASE}" "$((PIPE_DST_BASE + 1))" "$((PIPE_DST_BASE + 2))"
  if [[ "${KEEP_TMP:-0}" != "1" ]]; then
    rm -rf "${TMP_ROOT}"
  fi
  exit "${code}"
}
trap cleanup EXIT

rm -rf "${TMP_ROOT}"
mkdir -p "${TMP_ROOT}"

run_scenario() {
  local name=$1
  local mode=$2
  local src_base dst_base http_port
  local src_ports dst_ports src_csv dst_csv prefix conf_file target_master source_master

  if [[ "${mode}" == "sync" ]]; then
    src_base=${SYNC_SRC_BASE}
    dst_base=${SYNC_DST_BASE}
    http_port=${SYNC_HTTP_PORT}
  else
    src_base=${PIPE_SRC_BASE}
    dst_base=${PIPE_DST_BASE}
    http_port=${PIPE_HTTP_PORT}
  fi

  src_ports=("${src_base}" "$((src_base + 1))" "$((src_base + 2))")
  dst_ports=("${dst_base}" "$((dst_base + 1))" "$((dst_base + 2))")
  src_csv=$(format_addrs "${src_ports[@]}")
  dst_csv=$(format_addrs "${dst_ports[@]}")
  prefix="${TEST_PREFIX}:${name}"

  echo "[category8] scenario=${name} mode=${mode}"
  start_cluster "${REDIS_SERVER_BIN}" "${TMP_ROOT}" "${name}-src" "${src_ports[@]}"
  start_cluster "${REDIS_SERVER_BIN}" "${TMP_ROOT}" "${name}-dst" "${dst_ports[@]}"

  conf_file="${TMP_ROOT}/${name}.yaml"
  write_syncer_conf "${conf_file}" "${http_port}" "${src_csv}" cluster "${dst_csv}" cluster "${TMP_ROOT}/${name}-store" "${mode}"
  SYNCER_PID=$(start_syncer_process "${TMP_ROOT}" "${conf_file}" "${TMP_ROOT}/${name}.log")
  wait_for_syncer "${http_port}"
  sleep 2

  "${TMP_ROOT}/nonbisync_workload" \
    --scenario rich \
    --addrs "${src_csv}" \
    --prefix "${prefix}" \
    --key-space "${RICH_KEY_SETS}" \
    --report-json "${TMP_ROOT}/${name}.json" >/dev/null

  target_master=$(find_first_master_port "${dst_ports[@]}")
  redis_cmd cluster "${target_master}" set "nonbisync:cat8:isolated:${name}" "target-only"

  wait_for_cluster_equal "${TMP_ROOT}" "${src_csv}" "${dst_csv}" "${prefix}:stable:*"
  cluster_compare "${TMP_ROOT}" "${src_csv}" "${dst_csv}" "${prefix}:stable:*"
  assert_min_key_count_cluster "${prefix}:stable:*" "$((RICH_KEY_SETS * 16))" "${dst_ports[@]}"

  sleep 3
  wait_for_absent_db cluster "${target_master}" 0 "${prefix}:volatile:000000:ttl-short" "short ttl key on target"
  expect_eq "$(redis_call cluster "${target_master}" pttl "${prefix}:volatile:000000:ttl-persist")" "-1" "persisted ttl key on target"

  source_master=$(find_first_master_port "${src_ports[@]}")
  expect_absent "$(redis_call cluster "${source_master}" exists "nonbisync:cat8:isolated:${name}")" "isolated target key on source"
  assert_no_bisync_metadata_cluster "${dst_ports[@]}"
  assert_checkpoint_signals_cluster "${target_master}" "${dst_ports[@]}"
  assert_log_has_no_bisync_markers "${TMP_ROOT}/${name}.log"

  stop_pid "${SYNCER_PID}"
  SYNCER_PID=""
  shutdown_ports "${src_ports[@]}" "${dst_ports[@]}"
}

echo "[1/1] building binaries"
build_nonbisync_binaries "${TMP_ROOT}"

case ",${SCENARIOS}," in
  *",sync,"*)
    run_scenario sync sync
    ;;
esac
case ",${SCENARIOS}," in
  *",pipeline,"*)
    run_scenario pipeline pipeline
    ;;
esac
