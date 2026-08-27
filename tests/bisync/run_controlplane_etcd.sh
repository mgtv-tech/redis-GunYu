#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_ROOT="${TMPDIR:-/tmp}/redisgunyu-bisync-etcd"
source "${ROOT}/tests/nonbisync/lib/test_env.sh"

CONTROL_PLANE="${CONTROL_PLANE:-etcd}"
CONTROL_PLANE=$(printf '%s' "${CONTROL_PLANE}" | tr '[:upper:]' '[:lower:]')
if [[ "${CONTROL_PLANE}" != "etcd" && "${CONTROL_PLANE}" != "redis" ]]; then
  echo "unknown CONTROL_PLANE=${CONTROL_PLANE}; expected etcd or redis" >&2
  exit 1
fi
if [[ "${CONTROL_PLANE}" == "etcd" && "${ENABLE_ETCD_TESTS:-0}" != "1" ]]; then
  echo "etcd control-plane tests are disabled; set ENABLE_ETCD_TESTS=1 to run them"
  exit 0
fi
require_test_commands go redis-server redis-cli curl

ETCD_BIN="${ETCD_BIN:-$(command -v etcd || true)}"
ETCD_CLIENT_PORT="${ETCD_CLIENT_PORT:-23990}"
ETCD_PEER_PORT="${ETCD_PEER_PORT:-23991}"
LEFT_PORT="${LEFT_PORT:-36500}"
RIGHT_PORT="${RIGHT_PORT:-36600}"
FWD_HTTP_PORT="${FWD_HTTP_PORT:-36580}"
REV_HTTP_PORT="${REV_HTTP_PORT:-36680}"
REPLAY_MODE="${REPLAY_MODE:-sync}"
TEST_PREFIX="${TEST_PREFIX:-bisync:etcd:$(date +%s)}"
FWD_PID=""
REV_PID=""
ETCD_PID=""
REDIS_SERVER_BIN="$(resolve_redis_server_bin REDIS_SERVER_BIN REDIS_DEPLOY_ROOT)"

cleanup() {
  local code=$?
  set +e
  stop_pid "${FWD_PID:-}"
  stop_pid "${REV_PID:-}"
  stop_pid "${ETCD_PID:-}"
  shutdown_ports "${LEFT_PORT}" "${RIGHT_PORT}"
  if [[ "${KEEP_TMP:-0}" != "1" ]]; then
    rm -rf "${TMP_ROOT}"
  fi
  exit "${code}"
}
trap cleanup EXIT

if [[ "${CONTROL_PLANE}" == "etcd" && ( -z "${ETCD_BIN}" || ! -x "${ETCD_BIN}" ) ]]; then
  if [[ "${REQUIRE_ETCD_INTEGRATION:-0}" == "1" ]]; then
    echo "etcd binary not found but REQUIRE_ETCD_INTEGRATION=1" >&2
    exit 1
  fi
  echo "etcd binary not found; skipping bisync etcd control-plane test" >&2
  exit 0
fi

REPLAY_MODE=$(normalize_replay_mode "${REPLAY_MODE}")

rm -rf "${TMP_ROOT}"
mkdir -p "${TMP_ROOT}"

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

write_bisync_etcd_conf() {
  local file=$1
  local http_port=$2
  local src_port=$3
  local dst_port=$4
  local storer_dir=$5
  local group_name=$6

  mkdir -p "${storer_dir}"
  cat > "${file}" <<EOF
server:
  listen: 127.0.0.1:${http_port}
  listenPeer: 127.0.0.1:${http_port}
  gracefullStopTimeout: 1s
  checkRedisTypologyTicker: 2s
input:
  mode: static
  syncFrom: master
  syncDelayTestKey: redis-GunYu-syncDelay-testKey
  redis:
    type: standalone
    addresses: ["127.0.0.1:${src_port}"]
    version: "7.0.11"
channel:
  storer:
    dirPath: ${storer_dir}
    maxSize: 104857600
    logSize: 10485760
output:
  redis:
    type: standalone
    addresses: ["127.0.0.1:${dst_port}"]
    version: "7.0.11"
  replay:
    resumeFromBreakPoint: true
    keyExists: replace
    replayTransaction: true
    enableAofPipeline: false
    bisyncEnabled: true
    mode: ${REPLAY_MODE}
EOF
  if [[ "${CONTROL_PLANE}" == "etcd" ]]; then
    cat >> "${file}" <<EOF
cluster:
  groupName: ${group_name}
  leaseTimeout: 6s
  metaEtcd:
    endpoints: ["127.0.0.1:${ETCD_CLIENT_PORT}"]
EOF
  else
    cat >> "${file}" <<EOF
cluster:
  groupName: ${group_name}
  leaseTimeout: 6s
EOF
  fi
  cat >> "${file}" <<EOF
log:
  level: info
  handler:
    stdout: true
  withCaller: false
  withFunc: false
EOF
}

write_business_data() {
  local port=$1
  local prefix=$2
  local side=$3

  redis_cmd standalone "${port}" set "${prefix}:${side}:string" "value-${side}"
  redis_cmd standalone "${port}" incrby "${prefix}:${side}:counter" 7
  redis_cmd standalone "${port}" hset "${prefix}:${side}:hash" field1 "v-${side}-1" field2 "v-${side}-2"
  redis_cmd standalone "${port}" sadd "${prefix}:${side}:set" "${side}-red" "${side}-blue"
  redis_cmd standalone "${port}" rpush "${prefix}:${side}:list" "${side}-a" "${side}-b" "${side}-c"
  redis_cmd standalone "${port}" set "${prefix}:${side}:delete-me" "gone"
  redis_cmd standalone "${port}" del "${prefix}:${side}:delete-me"
}

assert_side_state() {
  local port=$1
  local prefix=$2
  local side=$3

  expect_eq "$(redis_call standalone "${port}" get "${prefix}:${side}:string")" "value-${side}" "${prefix}:${side}:string"
  expect_eq "$(redis_call standalone "${port}" get "${prefix}:${side}:counter")" "7" "${prefix}:${side}:counter"
  expect_eq "$(hash_state standalone "${port}" "${prefix}:${side}:hash")" "field1=v-${side}-1|field2=v-${side}-2" "${prefix}:${side}:hash"
  expect_eq "$(set_state standalone "${port}" "${prefix}:${side}:set")" "${side}-blue ${side}-red" "${prefix}:${side}:set"
  expect_eq "$(list_state standalone "${port}" "${prefix}:${side}:list")" "${side}-a ${side}-b ${side}-c" "${prefix}:${side}:list"
  expect_absent "$(redis_call standalone "${port}" exists "${prefix}:${side}:delete-me")" "${prefix}:${side}:delete-me"
}

wait_for_bisync_metadata_standalone() {
  local replay_mode=$1
  local port=$2
  local latest_count commit_count frontier_count

  for _ in $(seq 1 40); do
    latest_count=$(scan_count_standalone "${port}" 'redis-gunyu-bisync:*:latest:*')
    commit_count=$(scan_count_standalone "${port}" 'redis-gunyu-bisync:*:commit:*')
    frontier_count=$(scan_count_standalone "${port}" '*:frontier')

    if replay_mode_uses_frontier "${replay_mode}"; then
      if [[ "${frontier_count}" -gt 0 && "${latest_count}" == "0" && "${commit_count}" == "0" ]]; then
        return 0
      fi
    else
      if [[ "${latest_count}" -gt 0 && "${commit_count}" == "0" && "${frontier_count}" == "0" ]]; then
        return 0
      fi
    fi
    sleep 1
  done

  echo "bisync metadata did not settle on standalone target port ${port}: latest=${latest_count:-0} commit=${commit_count:-0} frontier=${frontier_count:-0}" >&2
  return 1
}

assert_log_has_no_role_probe_error() {
  local log_file=$1
  if match_regex_quiet 'cluster support disabled|get redis role error' "${log_file}"; then
    echo "unexpected role probe error in ${log_file}" >&2
    exit 1
  fi
}

echo "[1/3] building binaries"
build_nonbisync_binaries "${TMP_ROOT}"

if [[ "${CONTROL_PLANE}" == "etcd" ]]; then
  echo "[2/3] starting local etcd and standalone redis"
  start_etcd
else
  echo "[2/3] starting standalone redis with redis control plane"
fi
start_standalone "${REDIS_SERVER_BIN}" "${TMP_ROOT}" left "${LEFT_PORT}"
start_standalone "${REDIS_SERVER_BIN}" "${TMP_ROOT}" right "${RIGHT_PORT}"

write_bisync_etcd_conf "${TMP_ROOT}/forward.yaml" "${FWD_HTTP_PORT}" "${LEFT_PORT}" "${RIGHT_PORT}" "${TMP_ROOT}/forward-store" "bisync-etcd-forward"
write_bisync_etcd_conf "${TMP_ROOT}/reverse.yaml" "${REV_HTTP_PORT}" "${RIGHT_PORT}" "${LEFT_PORT}" "${TMP_ROOT}/reverse-store" "bisync-etcd-reverse"

echo "[3/3] running bisync with ${CONTROL_PLANE} control plane"
FWD_PID=$(start_syncer_process "${TMP_ROOT}" "${TMP_ROOT}/forward.yaml" "${TMP_ROOT}/forward.log")
REV_PID=$(start_syncer_process "${TMP_ROOT}" "${TMP_ROOT}/reverse.yaml" "${TMP_ROOT}/reverse.log")
wait_for_syncer "${FWD_HTTP_PORT}"
wait_for_syncer "${REV_HTTP_PORT}"

write_business_data "${LEFT_PORT}" "${TEST_PREFIX}" "left"
write_business_data "${RIGHT_PORT}" "${TEST_PREFIX}" "right"

wait_for_standalone_equal "${LEFT_PORT}" "${RIGHT_PORT}" "${TEST_PREFIX}:*" "${TMP_ROOT}"
wait_for_bisync_metadata_standalone "${REPLAY_MODE}" "${LEFT_PORT}"
wait_for_bisync_metadata_standalone "${REPLAY_MODE}" "${RIGHT_PORT}"

assert_side_state "${LEFT_PORT}" "${TEST_PREFIX}" "left"
assert_side_state "${LEFT_PORT}" "${TEST_PREFIX}" "right"
assert_side_state "${RIGHT_PORT}" "${TEST_PREFIX}" "left"
assert_side_state "${RIGHT_PORT}" "${TEST_PREFIX}" "right"
assert_log_has_no_role_probe_error "${TMP_ROOT}/forward.log"
assert_log_has_no_role_probe_error "${TMP_ROOT}/reverse.log"
