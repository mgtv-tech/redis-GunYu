#!/usr/bin/env bash
set -eEuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_ROOT="${TMPDIR:-/tmp}/redisgunyu-nonbisync-cat4"
source "${ROOT}/tests/nonbisync/lib/test_env.sh"

SCENARIOS="${SCENARIOS:-sync,pipeline}"
SYNC_SRC_BASE="${SYNC_SRC_BASE:-32300}"
SYNC_DST_BASE="${SYNC_DST_BASE:-32400}"
SYNC_HTTP_PORT="${SYNC_HTTP_PORT:-32380}"
PIPE_SRC_BASE="${PIPE_SRC_BASE:-32500}"
PIPE_DST_BASE="${PIPE_DST_BASE:-32600}"
PIPE_HTTP_PORT="${PIPE_HTTP_PORT:-32580}"
TEST_PREFIX="${TEST_PREFIX:-nonbisync:cat4:$(date +%s)}"
SYNCER_PID=""
REDIS_SERVER_BIN="$(resolve_redis_server_bin REDIS_SERVER_BIN REDIS_DEPLOY_ROOT)"
FORCE_KEEP_TMP=0
DIAG_CAPTURED=0
CURRENT_NAME=""
CURRENT_MODE=""
CURRENT_PREFIX=""
CURRENT_HTTP_PORT=""
CURRENT_LOG_FILE=""
CURRENT_SRC_CSV=""
CURRENT_DST_CSV=""
CURRENT_SOURCE_FAILOVER=""
CURRENT_TARGET_FAILOVER=""
CURRENT_SRC_PORTS=()
CURRENT_DST_PORTS=()

dump_cluster_group() {
  local label=$1
  shift
  local port
  echo "## ${label}"
  for port in "$@"; do
    cluster_port_snapshot "${port}" || true
  done
  echo
}

dump_expected_key_state() {
  local key=$1
  local source_master=$2
  local target_master=$3

  echo "### ${key}"
  echo "slot=$(cluster_key_slot "${source_master}" "${key}")"
  echo "source_master=${source_master}"
  dump_cluster_key_state "${source_master}" "${key}" || true
  echo "target_master=${target_master}"
  dump_cluster_key_state "${target_master}" "${key}" || true
  echo
}

capture_failure_diagnostics() {
  local code=$1
  if [[ "${code}" == "0" || "${DIAG_CAPTURED}" == "1" || -z "${CURRENT_NAME}" ]]; then
    return
  fi

  DIAG_CAPTURED=1
  FORCE_KEEP_TMP=1

  local report_file="${TMP_ROOT}/${CURRENT_NAME}.failure-report.txt"
  local source_master target_master isolated_key
  source_master=$(find_first_master_port "${CURRENT_SRC_PORTS[@]}" 2>/dev/null || true)
  target_master=$(find_first_master_port "${CURRENT_DST_PORTS[@]}" 2>/dev/null || true)
  isolated_key="nonbisync:cat4:isolated:${CURRENT_NAME}"

  {
    echo "# Category4 Failure Diagnostics"
    echo
    echo "generated_at=$(date '+%Y-%m-%d %H:%M:%S %z')"
    echo "scenario=${CURRENT_NAME}"
    echo "mode=${CURRENT_MODE}"
    echo "prefix=${CURRENT_PREFIX}"
    echo "http_port=${CURRENT_HTTP_PORT}"
    echo "sync_log=${CURRENT_LOG_FILE}"
    echo "source_failover=${CURRENT_SOURCE_FAILOVER}"
    echo "target_failover=${CURRENT_TARGET_FAILOVER}"
    echo
    echo "## Cluster Compare"
    cluster_compare "${TMP_ROOT}" "${CURRENT_SRC_CSV}" "${CURRENT_DST_CSV}" "${CURRENT_PREFIX}:*" || true
    echo
    dump_cluster_group "Source Ports" "${CURRENT_SRC_PORTS[@]}"
    dump_cluster_group "Target Ports" "${CURRENT_DST_PORTS[@]}"
    echo "## Expected Key States"
    if [[ -n "${source_master}" && -n "${target_master}" ]]; then
      dump_expected_key_state "$(key_name "${CURRENT_PREFIX}" "string")" "${source_master}" "${target_master}"
      dump_expected_key_state "$(key_name "${CURRENT_PREFIX}" "counter")" "${source_master}" "${target_master}"
      dump_expected_key_state "$(key_name "${CURRENT_PREFIX}" "set")" "${source_master}" "${target_master}"
      dump_expected_key_state "$(key_name "${CURRENT_PREFIX}" "hash")" "${source_master}" "${target_master}"
      dump_expected_key_state "$(key_name "${CURRENT_PREFIX}" "list")" "${source_master}" "${target_master}"
      dump_expected_key_state "$(key_name "${CURRENT_PREFIX}" "txn-counter{txn}")" "${source_master}" "${target_master}"
      dump_expected_key_state "$(key_name "${CURRENT_PREFIX}" "txn-list{txn}")" "${source_master}" "${target_master}"
      dump_expected_key_state "$(key_name "${CURRENT_PREFIX}" "txn-hash{txn}")" "${source_master}" "${target_master}"
      dump_expected_key_state "$(key_name "${CURRENT_PREFIX}" "delete-me")" "${source_master}" "${target_master}"
      dump_expected_key_state "${isolated_key}" "${source_master}" "${target_master}"
    else
      echo "master resolution failed: source_master=${source_master:-unknown} target_master=${target_master:-unknown}"
      echo
    fi
    echo "## Syncer Log Tail"
    tail -n 200 "${CURRENT_LOG_FILE}" || true
  } > "${report_file}"

  cat "${report_file}" >&2
  echo "failure_report=${report_file}" >&2
}

cleanup() {
  local code=$?
  set +e
  stop_pid "${SYNCER_PID:-}"
  shutdown_ports \
    "${SYNC_SRC_BASE}" "$((SYNC_SRC_BASE + 1))" "$((SYNC_SRC_BASE + 2))" "$((SYNC_SRC_BASE + 3))" "$((SYNC_SRC_BASE + 4))" "$((SYNC_SRC_BASE + 5))" \
    "${SYNC_DST_BASE}" "$((SYNC_DST_BASE + 1))" "$((SYNC_DST_BASE + 2))" "$((SYNC_DST_BASE + 3))" "$((SYNC_DST_BASE + 4))" "$((SYNC_DST_BASE + 5))" \
    "${PIPE_SRC_BASE}" "$((PIPE_SRC_BASE + 1))" "$((PIPE_SRC_BASE + 2))" "$((PIPE_SRC_BASE + 3))" "$((PIPE_SRC_BASE + 4))" "$((PIPE_SRC_BASE + 5))" \
    "${PIPE_DST_BASE}" "$((PIPE_DST_BASE + 1))" "$((PIPE_DST_BASE + 2))" "$((PIPE_DST_BASE + 3))" "$((PIPE_DST_BASE + 4))" "$((PIPE_DST_BASE + 5))"
  if [[ "${KEEP_TMP:-0}" != "1" && "${FORCE_KEEP_TMP}" != "1" ]]; then
    rm -rf "${TMP_ROOT}"
  fi
  exit "${code}"
}
trap 'capture_failure_diagnostics $?' ERR
trap cleanup EXIT

rm -rf "${TMP_ROOT}"
mkdir -p "${TMP_ROOT}"

key_name() {
  local prefix=$1
  local name=$2
  printf "%s:%s" "${prefix}" "${name}"
}

write_phase() {
  local port=$1
  local prefix=$2
  local phase=$3
  case "${phase}" in
    1)
      redis_cmd cluster "${port}" set "$(key_name "${prefix}" "string")" "p1"
      redis_cmd cluster "${port}" incrby "$(key_name "${prefix}" "counter")" 2
      redis_cmd cluster "${port}" sadd "$(key_name "${prefix}" "set")" red blue
      redis_cmd cluster "${port}" hset "$(key_name "${prefix}" "hash")" f1 v1
      redis_cmd cluster "${port}" rpush "$(key_name "${prefix}" "list")" a b
      ;;
    2)
      redis_cmd cluster "${port}" set "$(key_name "${prefix}" "string")" "p2"
      redis_cmd cluster "${port}" incrby "$(key_name "${prefix}" "counter")" 5
      redis_cmd cluster "${port}" sadd "$(key_name "${prefix}" "set")" green
      redis_cmd cluster "${port}" hset "$(key_name "${prefix}" "hash")" f2 v2
      redis_cmd cluster "${port}" rpush "$(key_name "${prefix}" "list")" c d
      ;;
    3)
      redis_cmd cluster "${port}" set "$(key_name "${prefix}" "string")" "p3"
      redis_cmd cluster "${port}" incrby "$(key_name "${prefix}" "counter")" 7
      redis_cmd cluster "${port}" sadd "$(key_name "${prefix}" "set")" yellow
      redis_cmd cluster "${port}" hset "$(key_name "${prefix}" "hash")" f3 v3
      redis_cmd cluster "${port}" rpush "$(key_name "${prefix}" "list")" e f
      ;;
    4)
      redis_cmd cluster "${port}" set "$(key_name "${prefix}" "string")" "p4"
      redis_cmd cluster "${port}" incrby "$(key_name "${prefix}" "counter")" 11
      redis_cmd cluster "${port}" sadd "$(key_name "${prefix}" "set")" white
      redis_cmd cluster "${port}" hset "$(key_name "${prefix}" "hash")" f4 v4
      redis_cmd cluster "${port}" set "$(key_name "${prefix}" "delete-me")" "gone"
      redis_cmd cluster "${port}" del "$(key_name "${prefix}" "delete-me")"
      redis-cli -c -p "${port}" <<EOF >/dev/null
MULTI
INCRBY $(key_name "${prefix}" "txn-counter{txn}") 4
RPUSH $(key_name "${prefix}" "txn-list{txn}") x y
HSET $(key_name "${prefix}" "txn-hash{txn}") field value
EXEC
EOF
      ;;
  esac
}

assert_expected_state() {
  local port=$1
  local prefix=$2

  expect_eq "$(redis_call cluster "${port}" get "$(key_name "${prefix}" "string")")" "p4" "$(key_name "${prefix}" "string")"
  expect_eq "$(redis_call cluster "${port}" get "$(key_name "${prefix}" "counter")")" "25" "$(key_name "${prefix}" "counter")"
  expect_eq "$(set_state cluster "${port}" "$(key_name "${prefix}" "set")")" "blue green red white yellow" "$(key_name "${prefix}" "set")"
  expect_eq "$(hash_state cluster "${port}" "$(key_name "${prefix}" "hash")")" "f1=v1|f2=v2|f3=v3|f4=v4" "$(key_name "${prefix}" "hash")"
  expect_eq "$(list_state cluster "${port}" "$(key_name "${prefix}" "list")")" "a b c d e f" "$(key_name "${prefix}" "list")"
  expect_eq "$(redis_call cluster "${port}" get "$(key_name "${prefix}" "txn-counter{txn}")")" "4" "$(key_name "${prefix}" "txn-counter{txn}")"
  expect_eq "$(list_state cluster "${port}" "$(key_name "${prefix}" "txn-list{txn}")")" "x y" "$(key_name "${prefix}" "txn-list{txn}")"
  expect_eq "$(hash_state cluster "${port}" "$(key_name "${prefix}" "txn-hash{txn}")")" "field=value" "$(key_name "${prefix}" "txn-hash{txn}")"
  expect_absent "$(redis_call cluster "${port}" exists "$(key_name "${prefix}" "delete-me")")" "$(key_name "${prefix}" "delete-me")"
}

run_scenario() {
  local name=$1
  local mode=$2
  local src_base dst_base http_port
  local src_ports dst_ports src_csv dst_csv prefix source_master source_replica target_replica target_master conf_file

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
  CURRENT_NAME="${name}"
  CURRENT_MODE="${mode}"
  CURRENT_PREFIX="${prefix}"
  CURRENT_HTTP_PORT="${http_port}"
  CURRENT_LOG_FILE="${TMP_ROOT}/${name}.log"
  CURRENT_SRC_CSV="${src_csv}"
  CURRENT_DST_CSV="${dst_csv}"
  CURRENT_SOURCE_FAILOVER=""
  CURRENT_TARGET_FAILOVER=""
  CURRENT_SRC_PORTS=("${src_ports[@]}")
  CURRENT_DST_PORTS=("${dst_ports[@]}")

  echo "[category4] scenario=${name} mode=${mode}"
  start_cluster_with_replicas "${REDIS_SERVER_BIN}" "${TMP_ROOT}" "${name}-src" "${src_ports[@]}"
  start_cluster_with_replicas "${REDIS_SERVER_BIN}" "${TMP_ROOT}" "${name}-dst" "${dst_ports[@]}"

  conf_file="${TMP_ROOT}/${name}.yaml"
  write_syncer_conf "${conf_file}" "${http_port}" "${src_csv}" cluster "${dst_csv}" cluster "${TMP_ROOT}/${name}-store" "${mode}"
  SYNCER_PID=$(start_syncer_process "${TMP_ROOT}" "${conf_file}" "${TMP_ROOT}/${name}.log")
  wait_for_syncer "${http_port}"
  sleep 2

  source_master=$(find_first_master_port "${src_ports[@]}")
  target_master=$(find_first_master_port "${dst_ports[@]}")
  write_phase "${source_master}" "${prefix}" 1
  wait_for_cluster_equal "${TMP_ROOT}" "${src_csv}" "${dst_csv}" "${prefix}:*"

  source_replica=$(find_first_replica_port "${src_ports[@]}")
  force_failover "${source_replica}"
  CURRENT_SOURCE_FAILOVER="${source_replica}"
  for port in "${src_ports[@]}"; do
    wait_for_cluster_ok "${port}"
  done
  source_master=$(find_first_master_port "${src_ports[@]}")
  write_phase "${source_master}" "${prefix}" 2
  wait_for_cluster_equal "${TMP_ROOT}" "${src_csv}" "${dst_csv}" "${prefix}:*"

  target_replica=$(find_first_replica_port "${dst_ports[@]}")
  force_failover "${target_replica}"
  CURRENT_TARGET_FAILOVER="${target_replica}"
  for port in "${dst_ports[@]}"; do
    wait_for_cluster_ok "${port}"
  done
  source_master=$(find_first_master_port "${src_ports[@]}")
  write_phase "${source_master}" "${prefix}" 3
  wait_for_cluster_equal "${TMP_ROOT}" "${src_csv}" "${dst_csv}" "${prefix}:*"

  restart_syncer_via_api "${http_port}"
  source_master=$(find_first_master_port "${src_ports[@]}")
  target_master=$(find_first_master_port "${dst_ports[@]}")
  redis_cmd cluster "${target_master}" set "nonbisync:cat4:isolated:${name}" "target-only"
  write_phase "${source_master}" "${prefix}" 4
  wait_for_cluster_equal "${TMP_ROOT}" "${src_csv}" "${dst_csv}" "${prefix}:*"
  cluster_compare "${TMP_ROOT}" "${src_csv}" "${dst_csv}" "${prefix}:*"

  target_master=$(find_first_master_port "${dst_ports[@]}")
  source_master=$(find_first_master_port "${src_ports[@]}")
  assert_expected_state "${target_master}" "${prefix}"
  expect_absent "$(redis_call cluster "${source_master}" exists "nonbisync:cat4:isolated:${name}")" "isolated target key on source"
  assert_no_bisync_metadata_cluster "${dst_ports[@]}"
  assert_checkpoint_signals_cluster "${target_master}" "${dst_ports[@]}"
  assert_log_has_no_bisync_markers "${TMP_ROOT}/${name}.log"

  stop_pid "${SYNCER_PID}"
  SYNCER_PID=""
  shutdown_ports "${src_ports[@]}" "${dst_ports[@]}"

  echo "prefix=${prefix}"
  echo "log=${TMP_ROOT}/${name}.log"
  echo "source_failover=${source_replica}"
  echo "target_failover=${target_replica}"
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
