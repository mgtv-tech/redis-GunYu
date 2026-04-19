#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_ROOT="${TMPDIR:-/tmp}/redisgunyu-nonbisync-etcd"
source "${ROOT}/tests/nonbisync/lib/test_env.sh"

ETCD_BIN="${ETCD_BIN:-$(command -v etcd || true)}"
ETCD_CLIENT_PORT="${ETCD_CLIENT_PORT:-23990}"
ETCD_PEER_PORT="${ETCD_PEER_PORT:-23991}"
SRC_PORT="${SRC_PORT:-36500}"
DST_PORT="${DST_PORT:-36600}"
HTTP_A="${HTTP_A:-36580}"
HTTP_B="${HTTP_B:-36680}"
TEST_PREFIX="${TEST_PREFIX:-nonbisync:etcd:$(date +%s)}"
SYNCER_PID_A=""
SYNCER_PID_B=""
ETCD_PID=""
REDIS_SERVER_BIN="$(resolve_redis_server_bin REDIS_SERVER_BIN REDIS_DEPLOY_ROOT)"

cleanup() {
  local code=$?
  set +e
  stop_pid "${SYNCER_PID_A:-}"
  stop_pid "${SYNCER_PID_B:-}"
  stop_pid "${ETCD_PID:-}"
  shutdown_ports "${SRC_PORT}" "${DST_PORT}"
  if [[ "${KEEP_TMP:-0}" != "1" ]]; then
    rm -rf "${TMP_ROOT}"
  fi
  exit "${code}"
}
trap cleanup EXIT

if [[ -z "${ETCD_BIN}" || ! -x "${ETCD_BIN}" ]]; then
  echo "etcd binary not found; skipping etcd control-plane test" >&2
  exit 0
fi

rm -rf "${TMP_ROOT}"
mkdir -p "${TMP_ROOT}"

key_name() {
  local prefix=$1
  local name=$2
  printf "%s:%s" "${prefix}" "${name}"
}

wait_for_etcd() {
  for _ in $(seq 1 80); do
    if curl -sf "http://127.0.0.1:${ETCD_CLIENT_PORT}/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  echo "etcd did not become ready" >&2
  return 1
}

start_etcd() {
  "${ETCD_BIN}" \
    --name default \
    --data-dir "${TMP_ROOT}/etcd-data" \
    --listen-client-urls "http://127.0.0.1:${ETCD_CLIENT_PORT}" \
    --advertise-client-urls "http://127.0.0.1:${ETCD_CLIENT_PORT}" \
    --listen-peer-urls "http://127.0.0.1:${ETCD_PEER_PORT}" \
    --initial-advertise-peer-urls "http://127.0.0.1:${ETCD_PEER_PORT}" \
    --initial-cluster "default=http://127.0.0.1:${ETCD_PEER_PORT}" \
    --initial-cluster-state new >"${TMP_ROOT}/etcd.log" 2>&1 &
  ETCD_PID=$!
  wait_for_etcd
}

write_etcd_conf() {
  local file=$1
  local http_port=$2
  local peer_port=$3
  local storer_dir=$4
  local mode=$5
  local group_name=$6

  mkdir -p "${storer_dir}"
  cat > "${file}" <<EOF
server:
  listen: 127.0.0.1:${http_port}
  listenPeer: 127.0.0.1:${peer_port}
  gracefullStopTimeout: 1s
  checkRedisTypologyTicker: 2s
input:
  redis:
    addresses: ["127.0.0.1:${SRC_PORT}"]
    type: standalone
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
    addresses: ["127.0.0.1:${DST_PORT}"]
    type: standalone
    version: "7.0.11"
  replay:
    resumeFromBreakPoint: true
    keyExists: replace
    metric: false
    targetDb: -1
    replayTransaction: true
    bisyncEnabled: false
    mode: ${mode}
cluster:
  groupName: ${group_name}
  leaseTimeout: 6s
  metaEtcd:
    endpoints: ["127.0.0.1:${ETCD_CLIENT_PORT}"]
log:
  level: info
  handler:
    stdout: true
  withCaller: false
  withFunc: false
EOF
}

pick_leader() {
  local log_a=$1
  local pid_a=$2
  local log_b=$3
  local pid_b=$4
  if rg -q 'new_role\(leader\)|RunLeader' "${log_a}"; then
    printf '%s|%s\n' "${pid_a}" "${log_a}"
  elif rg -q 'new_role\(leader\)|RunLeader' "${log_b}"; then
    printf '%s|%s\n' "${pid_b}" "${log_b}"
  else
    return 1
  fi
}

wait_for_leader_pick() {
  local log_a=$1
  local pid_a=$2
  local log_b=$3
  local pid_b=$4
  local timeout_seconds=${5:-20}
  local waited=0
  local leader_info

  while (( waited < timeout_seconds * 4 )); do
    if leader_info=$(pick_leader "${log_a}" "${pid_a}" "${log_b}" "${pid_b}" 2>/dev/null); then
      printf '%s\n' "${leader_info}"
      return 0
    fi
    sleep 0.25
    waited=$((waited + 1))
  done

  echo "leader log signal not found within ${timeout_seconds}s" >&2
  return 1
}

echo "[1/1] building binaries"
build_nonbisync_binaries "${TMP_ROOT}"
start_etcd
start_standalone "${REDIS_SERVER_BIN}" "${TMP_ROOT}" src "${SRC_PORT}"
start_standalone "${REDIS_SERVER_BIN}" "${TMP_ROOT}" dst "${DST_PORT}"

write_etcd_conf "${TMP_ROOT}/a.yaml" "${HTTP_A}" "${HTTP_A}" "${TMP_ROOT}/a-store" sync nonbisync-etcd
write_etcd_conf "${TMP_ROOT}/b.yaml" "${HTTP_B}" "${HTTP_B}" "${TMP_ROOT}/b-store" sync nonbisync-etcd

SYNCER_PID_A=$(start_syncer_process "${TMP_ROOT}" "${TMP_ROOT}/a.yaml" "${TMP_ROOT}/a.log")
SYNCER_PID_B=$(start_syncer_process "${TMP_ROOT}" "${TMP_ROOT}/b.yaml" "${TMP_ROOT}/b.log")
wait_for_syncer "${HTTP_A}"
wait_for_syncer "${HTTP_B}"
wait_for_log_pattern "${TMP_ROOT}/a.log" 'new_role\(leader\)|new_role\(follower\)|RunLeader|RunFollower' 20 || true
wait_for_log_pattern "${TMP_ROOT}/b.log" 'new_role\(leader\)|new_role\(follower\)|RunLeader|RunFollower' 20 || true

leader_info=$(wait_for_leader_pick "${TMP_ROOT}/a.log" "${SYNCER_PID_A}" "${TMP_ROOT}/b.log" "${SYNCER_PID_B}" 20)
leader_pid=${leader_info%%|*}
leader_log=${leader_info#*|}
if [[ "${leader_pid}" == "${SYNCER_PID_A}" ]]; then
  follower_log="${TMP_ROOT}/b.log"
else
  follower_log="${TMP_ROOT}/a.log"
fi

redis_cmd standalone "${SRC_PORT}" set "$(key_name "${TEST_PREFIX}" "string")" "p1"
wait_for_standalone_equal "${SRC_PORT}" "${DST_PORT}" "${TEST_PREFIX}:*" "${TMP_ROOT}"
stop_pid "${leader_pid}"
if [[ "${leader_pid}" == "${SYNCER_PID_A}" ]]; then
  SYNCER_PID_A=""
else
  SYNCER_PID_B=""
fi
wait_for_log_pattern "${follower_log}" 'new_role\(leader\)|RunLeader' 20
redis_cmd standalone "${SRC_PORT}" set "$(key_name "${TEST_PREFIX}" "string")" "p2"
redis_cmd standalone "${DST_PORT}" set "nonbisync:etcd:isolated" "target-only"
wait_for_standalone_equal "${SRC_PORT}" "${DST_PORT}" "${TEST_PREFIX}:*" "${TMP_ROOT}"
expect_eq "$(redis_call standalone "${DST_PORT}" get "$(key_name "${TEST_PREFIX}" "string")")" "p2" "$(key_name "${TEST_PREFIX}" "string")"
expect_absent "$(redis_call standalone "${SRC_PORT}" exists "nonbisync:etcd:isolated")" "isolated target key on source"
assert_no_bisync_metadata_standalone "${DST_PORT}"
assert_checkpoint_signals_standalone "${DST_PORT}"
assert_log_has_no_bisync_markers "${leader_log}"
assert_log_has_no_bisync_markers "${follower_log}"
