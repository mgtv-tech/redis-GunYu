#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_ROOT="${TMPDIR:-/tmp}/redisgunyu-nonbisync-cat10"
source "${ROOT}/tests/nonbisync/lib/test_env.sh"

SCENARIOS="${SCENARIOS:-sync,pipeline}"
SYNC_SRC_BASE="${SYNC_SRC_BASE:-35500}"
SYNC_DST_PORT="${SYNC_DST_PORT:-35600}"
SYNC_HTTP_A="${SYNC_HTTP_A:-35580}"
SYNC_HTTP_B="${SYNC_HTTP_B:-35680}"
PIPE_SRC_BASE="${PIPE_SRC_BASE:-35700}"
PIPE_DST_PORT="${PIPE_DST_PORT:-35800}"
PIPE_HTTP_A="${PIPE_HTTP_A:-35780}"
PIPE_HTTP_B="${PIPE_HTTP_B:-35880}"
TEST_PREFIX="${TEST_PREFIX:-nonbisync:cat10:$(date +%s)}"
SYNCER_PID_A=""
SYNCER_PID_B=""
REDIS_SERVER_BIN="$(resolve_redis_server_bin REDIS_SERVER_BIN REDIS_DEPLOY_ROOT)"

cleanup() {
  local code=$?
  set +e
  stop_pid "${SYNCER_PID_A:-}"
  stop_pid "${SYNCER_PID_B:-}"
  shutdown_ports \
    "${SYNC_SRC_BASE}" "$((SYNC_SRC_BASE + 1))" "$((SYNC_SRC_BASE + 2))" "$((SYNC_SRC_BASE + 3))" "$((SYNC_SRC_BASE + 4))" "$((SYNC_SRC_BASE + 5))" \
    "${SYNC_DST_PORT}" \
    "${PIPE_SRC_BASE}" "$((PIPE_SRC_BASE + 1))" "$((PIPE_SRC_BASE + 2))" "$((PIPE_SRC_BASE + 3))" "$((PIPE_SRC_BASE + 4))" "$((PIPE_SRC_BASE + 5))" \
    "${PIPE_DST_PORT}"
  if [[ "${KEEP_TMP:-0}" != "1" ]]; then
    rm -rf "${TMP_ROOT}"
  fi
  exit "${code}"
}
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
      redis_cmd cluster "${port}" incrby "$(key_name "${prefix}" "counter")" 3
      ;;
    2)
      redis_cmd cluster "${port}" set "$(key_name "${prefix}" "string")" "p2"
      redis_cmd cluster "${port}" incrby "$(key_name "${prefix}" "counter")" 4
      redis_cmd cluster "${port}" hset "$(key_name "${prefix}" "hash")" f1 v1 f2 v2
      ;;
  esac
}

write_ha_conf() {
  local file=$1
  local http_port=$2
  local peer_port=$3
  local src_addrs=$4
  local dst_port=$5
  local storer_dir=$6
  local replay_mode=$7
  local group_name=$8

  mkdir -p "${storer_dir}"
  cat > "${file}" <<EOF
server:
  listen: 127.0.0.1:${http_port}
  listenPeer: 127.0.0.1:${peer_port}
  gracefullStopTimeout: 1s
  checkRedisTypologyTicker: 2s
input:
  redis:
    addresses: [${src_addrs}]
    type: cluster
    version: "7.0.11"
  mode: dynamic
  syncFrom: master
channel:
  storer:
    dirPath: ${storer_dir}
    maxSize: 104857600
    logSize: 10485760
  staleCheckpointDuration: 10m
output:
  redis:
    addresses: ["127.0.0.1:${dst_port}"]
    type: standalone
    version: "7.0.11"
  replay:
    resumeFromBreakPoint: true
    keyExists: replace
    metric: false
    targetDb: -1
    replayTransaction: true
    bisyncEnabled: false
    mode: ${replay_mode}
cluster:
  groupName: ${group_name}
  leaseTimeout: 6s
log:
  level: info
  handler:
    stdout: true
  withCaller: false
  withFunc: false
EOF
}

assert_expected_state() {
  local port=$1
  local prefix=$2
  expect_eq "$(redis_call standalone "${port}" get "$(key_name "${prefix}" "string")")" "p2" "$(key_name "${prefix}" "string")"
  expect_eq "$(redis_call standalone "${port}" get "$(key_name "${prefix}" "counter")")" "7" "$(key_name "${prefix}" "counter")"
  expect_eq "$(hash_state standalone "${port}" "$(key_name "${prefix}" "hash")")" "f1=v1|f2=v2" "$(key_name "${prefix}" "hash")"
}

wait_for_phase1_state() {
  local port=$1
  local prefix=$2
  for _ in $(seq 1 80); do
    if [[ "$(redis_call standalone "${port}" get "$(key_name "${prefix}" "string")" 2>/dev/null || true)" == "p1" ]] && \
       [[ "$(redis_call standalone "${port}" get "$(key_name "${prefix}" "counter")" 2>/dev/null || true)" == "3" ]]; then
      return 0
    fi
    sleep 0.5
  done
  echo "standalone target did not reach phase1 state for ${prefix}" >&2
  return 1
}

wait_for_expected_state() {
  local port=$1
  local prefix=$2
  for _ in $(seq 1 80); do
    if [[ "$(redis_call standalone "${port}" get "$(key_name "${prefix}" "string")" 2>/dev/null || true)" == "p2" ]] && \
       [[ "$(redis_call standalone "${port}" get "$(key_name "${prefix}" "counter")" 2>/dev/null || true)" == "7" ]] && \
       [[ "$(hash_state standalone "${port}" "$(key_name "${prefix}" "hash")" 2>/dev/null || true)" == "f1=v1|f2=v2" ]]; then
      return 0
    fi
    sleep 0.5
  done
  echo "standalone target did not reach final expected state for ${prefix}" >&2
  return 1
}

run_scenario() {
  local name=$1
  local mode=$2
  local src_base dst_port http_a http_b
  local src_ports src_csv prefix conf_a conf_b group_name source_master

  if [[ "${mode}" == "sync" ]]; then
    src_base=${SYNC_SRC_BASE}
    dst_port=${SYNC_DST_PORT}
    http_a=${SYNC_HTTP_A}
    http_b=${SYNC_HTTP_B}
  else
    src_base=${PIPE_SRC_BASE}
    dst_port=${PIPE_DST_PORT}
    http_a=${PIPE_HTTP_A}
    http_b=${PIPE_HTTP_B}
  fi

  src_ports=("${src_base}" "$((src_base + 1))" "$((src_base + 2))" "$((src_base + 3))" "$((src_base + 4))" "$((src_base + 5))")
  src_csv=$(format_addrs "${src_ports[@]}")
  prefix="${TEST_PREFIX}:${name}"
  group_name="nonbisync-cat10-${name}"

  echo "[category10] scenario=${name} mode=${mode}"
  start_cluster_with_replicas "${REDIS_SERVER_BIN}" "${TMP_ROOT}" "${name}-src" "${src_ports[@]}"
  start_standalone "${REDIS_SERVER_BIN}" "${TMP_ROOT}" "${name}-dst" "${dst_port}"

  conf_a="${TMP_ROOT}/${name}-a.yaml"
  conf_b="${TMP_ROOT}/${name}-b.yaml"
  write_ha_conf "${conf_a}" "${http_a}" "${http_a}" "${src_csv}" "${dst_port}" "${TMP_ROOT}/${name}-a-store" "${mode}" "${group_name}"
  write_ha_conf "${conf_b}" "${http_b}" "${http_b}" "${src_csv}" "${dst_port}" "${TMP_ROOT}/${name}-b-store" "${mode}" "${group_name}"

  SYNCER_PID_A=$(start_syncer_process "${TMP_ROOT}" "${conf_a}" "${TMP_ROOT}/${name}-a.log")
  wait_for_syncer "${http_a}"
  wait_for_log_pattern "${TMP_ROOT}/${name}-a.log" 'new_role\(leader\)|RunLeader' 20

  SYNCER_PID_B=$(start_syncer_process "${TMP_ROOT}" "${conf_b}" "${TMP_ROOT}/${name}-b.log")
  wait_for_syncer "${http_b}"
  wait_for_log_pattern "${TMP_ROOT}/${name}-b.log" 'new_role\(follower\)|RunFollower' 20

  source_master=$(find_first_master_port "${src_ports[@]}")
  write_phase "${source_master}" "${prefix}" 1
  wait_for_phase1_state "${dst_port}" "${prefix}"

  stop_pid "${SYNCER_PID_A}"
  SYNCER_PID_A=""
  wait_for_log_pattern "${TMP_ROOT}/${name}-b.log" 'new_role\(leader\)|RunLeader' 20

  source_master=$(find_first_master_port "${src_ports[@]}")
  write_phase "${source_master}" "${prefix}" 2
  redis_cmd standalone "${dst_port}" set "nonbisync:cat10:isolated:${name}" "target-only"
  wait_for_expected_state "${dst_port}" "${prefix}"

  assert_expected_state "${dst_port}" "${prefix}"
  expect_absent "$(redis_call cluster "${source_master}" exists "nonbisync:cat10:isolated:${name}")" "isolated target key on source"
  assert_no_bisync_metadata_standalone "${dst_port}"
  assert_checkpoint_signals_standalone "${dst_port}"
  assert_log_has_no_bisync_markers "${TMP_ROOT}/${name}-a.log"
  assert_log_has_no_bisync_markers "${TMP_ROOT}/${name}-b.log"

  stop_pid "${SYNCER_PID_A:-}"
  stop_pid "${SYNCER_PID_B:-}"
  SYNCER_PID_A=""
  SYNCER_PID_B=""
  shutdown_ports "${src_ports[@]}" "${dst_port}"
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
