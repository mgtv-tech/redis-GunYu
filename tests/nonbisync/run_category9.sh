#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_ROOT="${TMPDIR:-/tmp}/redisgunyu-nonbisync-cat9"
source "${ROOT}/tests/nonbisync/lib/test_env.sh"
require_test_commands go redis-server redis-cli curl

SCENARIOS="${SCENARIOS:-sync,pipeline}"
SOAK_DURATION_SECONDS="${SOAK_DURATION_SECONDS:-75}"
SOAK_KEY_SPACE="${SOAK_KEY_SPACE:-64}"
SOAK_TARGET_QPS="${SOAK_TARGET_QPS:-4000}"
SOAK_WORKERS="${SOAK_WORKERS:-2}"
SOAK_BOUNDARY_EVERY="${SOAK_BOUNDARY_EVERY:-24}"
SOAK_VOLATILE_EVERY="${SOAK_VOLATILE_EVERY:-20}"
SOAK_TXN_EVERY="${SOAK_TXN_EVERY:-12}"
SOAK_SCRIPT_EVERY="${SOAK_SCRIPT_EVERY:-18}"
SOAK_OFFLINE_SECONDS="${SOAK_OFFLINE_SECONDS:-8}"
FINAL_SETTLE_SECONDS="${FINAL_SETTLE_SECONDS:-12}"
MONITOR_INTERVAL_SECONDS="${MONITOR_INTERVAL_SECONDS:-5}"
SYNC_SRC_BASE="${SYNC_SRC_BASE:-35100}"
SYNC_DST_BASE="${SYNC_DST_BASE:-35200}"
SYNC_HTTP_PORT="${SYNC_HTTP_PORT:-35180}"
PIPE_SRC_BASE="${PIPE_SRC_BASE:-35300}"
PIPE_DST_BASE="${PIPE_DST_BASE:-35400}"
PIPE_HTTP_PORT="${PIPE_HTTP_PORT:-35380}"
TEST_PREFIX="${TEST_PREFIX:-nonbisync:cat9:$(date +%s)}"
SYNCER_PID=""
MONITOR_PID=""
REDIS_SERVER_BIN="$(resolve_redis_server_bin REDIS_SERVER_BIN REDIS_DEPLOY_ROOT)"

cleanup() {
  local code=$?
  set +e
  stop_pid "${MONITOR_PID:-}"
  stop_pid "${SYNCER_PID:-}"
  shutdown_ports \
    "${SYNC_SRC_BASE}" "$((SYNC_SRC_BASE + 1))" "$((SYNC_SRC_BASE + 2))" "$((SYNC_SRC_BASE + 3))" "$((SYNC_SRC_BASE + 4))" "$((SYNC_SRC_BASE + 5))" \
    "${SYNC_DST_BASE}" "$((SYNC_DST_BASE + 1))" "$((SYNC_DST_BASE + 2))" "$((SYNC_DST_BASE + 3))" "$((SYNC_DST_BASE + 4))" "$((SYNC_DST_BASE + 5))" \
    "${PIPE_SRC_BASE}" "$((PIPE_SRC_BASE + 1))" "$((PIPE_SRC_BASE + 2))" "$((PIPE_SRC_BASE + 3))" "$((PIPE_SRC_BASE + 4))" "$((PIPE_SRC_BASE + 5))" \
    "${PIPE_DST_BASE}" "$((PIPE_DST_BASE + 1))" "$((PIPE_DST_BASE + 2))" "$((PIPE_DST_BASE + 3))" "$((PIPE_DST_BASE + 4))" "$((PIPE_DST_BASE + 5))"
  if [[ "${KEEP_TMP:-0}" != "1" ]]; then
    rm -rf "${TMP_ROOT}"
  fi
  exit "${code}"
}
trap cleanup EXIT

rm -rf "${TMP_ROOT}"
mkdir -p "${TMP_ROOT}"

start_monitor() {
  local syncer_pid=$1
  local http_port=$2
  local out_file=$3
  local append_mode
  if [[ -s "${out_file}" ]]; then
    append_mode=1
  else
    append_mode=0
  fi
  {
    if [[ "${append_mode}" == "0" ]]; then
      echo -e "ts\trss_kb\tgoroutines"
    fi
    while kill -0 "${syncer_pid}" >/dev/null 2>&1; do
      local ts rss goroutines
      ts=$(date '+%Y-%m-%dT%H:%M:%S%z')
      rss=$(ps -o rss= -p "${syncer_pid}" 2>/dev/null | awk '{print $1}' || true)
      goroutines=$(curl -sf "http://127.0.0.1:${http_port}/debug/pprof/goroutine?debug=1" 2>/dev/null | count_regex_matches '^goroutine ' || true)
      echo -e "${ts}\t${rss:-0}\t${goroutines:-0}"
      sleep "${MONITOR_INTERVAL_SECONDS}"
    done
  } >> "${out_file}" &
  MONITOR_PID=$!
}

run_workload_segment() {
  local src_csv=$1
  local prefix=$2
  local duration_seconds=$3
  local report_json=$4
  local workload_log=$5

  "${TMP_ROOT}/nonbisync_workload" \
    --scenario soak \
    --addrs "${src_csv}" \
    --prefix "${prefix}" \
    --duration "${duration_seconds}s" \
    --key-space "${SOAK_KEY_SPACE}" \
    --target-qps "${SOAK_TARGET_QPS}" \
    --workers "${SOAK_WORKERS}" \
    --boundary-every "${SOAK_BOUNDARY_EVERY}" \
    --volatile-every "${SOAK_VOLATILE_EVERY}" \
    --txn-every "${SOAK_TXN_EVERY}" \
    --script-every "${SOAK_SCRIPT_EVERY}" \
    --report-json "${report_json}" >>"${workload_log}"
}

run_scenario() {
  local name=$1
  local mode=$2
  local src_base dst_base http_port
  local src_ports dst_ports src_csv dst_csv prefix target_master source_master
  local phase1_seconds phase2_seconds phase4_seconds offline_seconds
  local src_replica dst_replica

  if [[ "${mode}" == "sync" ]]; then
    src_base=${SYNC_SRC_BASE}
    dst_base=${SYNC_DST_BASE}
    http_port=${SYNC_HTTP_PORT}
  else
    src_base=${PIPE_SRC_BASE}
    dst_base=${PIPE_DST_BASE}
    http_port=${PIPE_HTTP_PORT}
  fi

  src_ports=("${src_base}" "$((src_base + 1))" "$((src_base + 2))" "$((src_base + 3))" "$((src_base + 4))" "$((src_base + 5))")
  dst_ports=("${dst_base}" "$((dst_base + 1))" "$((dst_base + 2))" "$((dst_base + 3))" "$((dst_base + 4))" "$((dst_base + 5))")
  src_csv=$(format_addrs "${src_ports[@]}")
  dst_csv=$(format_addrs "${dst_ports[@]}")
  prefix="${TEST_PREFIX}:${name}"

  phase1_seconds=$((SOAK_DURATION_SECONDS * 2 / 5))
  phase2_seconds=$((SOAK_DURATION_SECONDS / 5))
  phase4_seconds=$((SOAK_DURATION_SECONDS - phase1_seconds - phase2_seconds))
  offline_seconds=${SOAK_OFFLINE_SECONDS}

  if (( phase1_seconds < 10 )); then
    phase1_seconds=10
  fi
  if (( phase2_seconds < 8 )); then
    phase2_seconds=8
  fi
  if (( phase4_seconds < 10 )); then
    phase4_seconds=10
  fi
  if (( offline_seconds < 3 )); then
    offline_seconds=3
  fi

  echo "[category9] scenario=${name} mode=${mode}"
  start_cluster_with_replicas "${REDIS_SERVER_BIN}" "${TMP_ROOT}" "${name}-src" "${src_ports[@]}"
  start_cluster_with_replicas "${REDIS_SERVER_BIN}" "${TMP_ROOT}" "${name}-dst" "${dst_ports[@]}"

  write_syncer_conf "${TMP_ROOT}/${name}.yaml" "${http_port}" "${src_csv}" cluster "${dst_csv}" cluster "${TMP_ROOT}/${name}-store" "${mode}"
  SYNCER_PID=$(start_syncer_process "${TMP_ROOT}" "${TMP_ROOT}/${name}.yaml" "${TMP_ROOT}/${name}.log")
  wait_for_syncer "${http_port}"
  sleep 2
  start_monitor "${SYNCER_PID}" "${http_port}" "${TMP_ROOT}/${name}.resources.tsv"

  : > "${TMP_ROOT}/${name}.workload.log"

  run_workload_segment "${src_csv}" "${prefix}" "${phase1_seconds}" "${TMP_ROOT}/${name}.phase1.json" "${TMP_ROOT}/${name}.workload.log"
  sleep 2

  src_replica=$(find_first_replica_port "${src_ports[@]}")
  force_failover "${src_replica}"
  for port in "${src_ports[@]}"; do
    wait_for_cluster_ok "${port}"
  done

  run_workload_segment "${src_csv}" "${prefix}" "${phase2_seconds}" "${TMP_ROOT}/${name}.phase2.json" "${TMP_ROOT}/${name}.workload.log"
  sleep 2

  dst_replica=$(find_first_replica_port "${dst_ports[@]}")
  force_failover "${dst_replica}"
  for port in "${dst_ports[@]}"; do
    wait_for_cluster_ok "${port}"
  done

  restart_syncer_via_api "${http_port}"

  stop_pid "${SYNCER_PID}"
  SYNCER_PID=""
  stop_pid "${MONITOR_PID:-}"
  MONITOR_PID=""

  run_workload_segment "${src_csv}" "${prefix}" "${offline_seconds}" "${TMP_ROOT}/${name}.offline.json" "${TMP_ROOT}/${name}.workload.log"

  SYNCER_PID=$(start_syncer_process "${TMP_ROOT}" "${TMP_ROOT}/${name}.yaml" "${TMP_ROOT}/${name}.restart.log")
  wait_for_syncer "${http_port}"
  start_monitor "${SYNCER_PID}" "${http_port}" "${TMP_ROOT}/${name}.resources.tsv"
  sleep 2

  run_workload_segment "${src_csv}" "${prefix}" "${phase4_seconds}" "${TMP_ROOT}/${name}.json" "${TMP_ROOT}/${name}.workload.log"

  sleep "${FINAL_SETTLE_SECONDS}"
  wait_for_cluster_equal "${TMP_ROOT}" "${src_csv}" "${dst_csv}" "${prefix}:stable:*"
  cluster_compare "${TMP_ROOT}" "${src_csv}" "${dst_csv}" "${prefix}:stable:*"

  target_master=$(find_first_master_port "${dst_ports[@]}")
  source_master=$(find_first_master_port "${src_ports[@]}")
  redis_cmd cluster "${target_master}" set "nonbisync:cat9:isolated:${name}" "target-only"
  expect_absent "$(redis_call cluster "${source_master}" exists "nonbisync:cat9:isolated:${name}")" "isolated target key on source"
  assert_no_bisync_metadata_cluster "${dst_ports[@]}"
  assert_checkpoint_signals_cluster "${target_master}" "${dst_ports[@]}"
  assert_log_has_no_bisync_markers "${TMP_ROOT}/${name}.log"
  assert_log_has_no_bisync_markers "${TMP_ROOT}/${name}.restart.log"
  assert_log_has_topology_signal "${TMP_ROOT}/${name}.log"
  assert_log_has_resume_signal "${TMP_ROOT}/${name}.restart.log"

  stop_pid "${MONITOR_PID:-}"
  MONITOR_PID=""
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
