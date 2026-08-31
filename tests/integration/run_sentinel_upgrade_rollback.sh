#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/nonbisync/lib/test_env.sh"
require_test_commands go redis-server redis-cli curl

PREVIOUS_GUNYU_BIN="${PREVIOUS_GUNYU_BIN:?PREVIOUS_GUNYU_BIN is required}"
REDIS_SERVER_BIN="$(resolve_redis_server_bin REDIS_SERVER_BIN REDIS_DEPLOY_ROOT)"
PORT_BASE="${SENTINEL_UPGRADE_PORT_BASE:-43700}"
TMP_ROOT="${TMPDIR:-/tmp}/redisgunyu-sentinel-upgrade-rollback"
SRC_MASTER=$((PORT_BASE + 0)); SRC_REPLICA=$((PORT_BASE + 1))
SRC_SENTINELS=("$((PORT_BASE + 2))" "$((PORT_BASE + 3))" "$((PORT_BASE + 4))")
DST_MASTER=$((PORT_BASE + 5)); DST_REPLICA=$((PORT_BASE + 6))
DST_SENTINELS=("$((PORT_BASE + 7))" "$((PORT_BASE + 8))" "$((PORT_BASE + 9))")
HTTP_PORT=$((PORT_BASE + 10))
DATA_PASSWORD="sentinel-upgrade-data-password"
SENTINEL_PASSWORD="sentinel-upgrade-sentinel-password"
SRC_NAME="gunyu-upgrade-source"; DST_NAME="gunyu-upgrade-target"
SYNCER_PID=""

stop_owned_processes() {
  local pid_file pid
  while IFS= read -r pid_file; do
    [[ -f "${pid_file}" ]] || continue
    pid=$(tr -d '[:space:]' <"${pid_file}")
    if [[ "${pid}" =~ ^[0-9]+$ ]] && kill -0 "${pid}" >/dev/null 2>&1; then kill "${pid}" >/dev/null 2>&1 || true; fi
  done < <(find "${TMP_ROOT}" -type f -name redis.pid 2>/dev/null)
}
cleanup() { local code=$?; set +e; stop_pid "${SYNCER_PID:-}"; stop_owned_processes; exit "${code}"; }
trap cleanup EXIT
mkdir -p "${TMP_ROOT}"

redis_data() { local port=$1; shift; REDISCLI_AUTH="${DATA_PASSWORD}" redis-cli -p "${port}" --raw "$@"; }
redis_sentinel() { local port=$1; shift; REDISCLI_AUTH="${SENTINEL_PASSWORD}" redis-cli -p "${port}" --raw "$@"; }
wait_data() { local port=$1; for _ in $(seq 1 100); do redis_data "${port}" ping 2>/dev/null | match_regex_quiet '^PONG$' && return 0; sleep 0.1; done; return 1; }
wait_sentinel() { local port=$1; for _ in $(seq 1 100); do redis_sentinel "${port}" ping 2>/dev/null | match_regex_quiet '^PONG$' && return 0; sleep 0.1; done; return 1; }
wait_replica() { local port=$1; for _ in $(seq 1 150); do redis_data "${port}" info replication 2>/dev/null | match_regex_quiet '^master_link_status:up' && return 0; sleep 0.1; done; return 1; }

start_data_group() {
  local prefix=$1 master=$2 replica=$3
  local master_dir="${TMP_ROOT}/${prefix}-master-${master}"
  local replica_dir="${TMP_ROOT}/${prefix}-replica-${replica}"
  write_standalone_conf "${master_dir}" "${master}" "requirepass ${DATA_PASSWORD}
masterauth ${DATA_PASSWORD}"
  "${REDIS_SERVER_BIN}" "${master_dir}/redis.conf"; wait_data "${master}"
  write_standalone_conf "${replica_dir}" "${replica}" "requirepass ${DATA_PASSWORD}
masterauth ${DATA_PASSWORD}
replicaof 127.0.0.1 ${master}"
  "${REDIS_SERVER_BIN}" "${replica_dir}/redis.conf"; wait_data "${replica}"; wait_replica "${replica}"
}

start_sentinels() {
  local prefix=$1 name=$2 master=$3; shift 3
  local port dir
  for port in "$@"; do
    dir="${TMP_ROOT}/${prefix}-sentinel-${port}"; mkdir -p "${dir}"
    cat >"${dir}/sentinel.conf" <<EOF
port ${port}
bind 127.0.0.1
protected-mode no
daemonize yes
dir ${dir}
pidfile ${dir}/redis.pid
logfile ${dir}/redis.log
requirepass ${SENTINEL_PASSWORD}
sentinel monitor ${name} 127.0.0.1 ${master} 2
sentinel auth-pass ${name} ${DATA_PASSWORD}
sentinel down-after-milliseconds ${name} 500
sentinel failover-timeout ${name} 5000
sentinel parallel-syncs ${name} 1
EOF
    "${REDIS_SERVER_BIN}" "${dir}/sentinel.conf" --sentinel; wait_sentinel "${port}"
  done
}

sentinel_master_port() { redis_sentinel "$1" sentinel get-master-addr-by-name "$2" | tail -n 1; }
wait_master_change() {
  local sentinel=$1 name=$2 old=$3 current
  for _ in $(seq 1 250); do current=$(sentinel_master_port "${sentinel}" "${name}" 2>/dev/null || true); [[ -n "${current}" && "${current}" != "${old}" ]] && { printf '%s\n' "${current}"; return 0; }; sleep 0.1; done
  return 1
}
wait_value() {
  local port=$1 key=$2 value=$3 actual
  for _ in $(seq 1 200); do actual=$(redis_data "${port}" get "${key}" 2>/dev/null || true); [[ "${actual}" == "${value}" ]] && return 0; sleep 0.1; done
  echo "target ${port} did not converge: ${key}=${actual:-empty}, expected ${value}" >&2; return 1
}
start_version() {
  local binary=$1 conf=$2 label=$3
  "${binary}" -conf "${conf}" -cmd sync >"${TMP_ROOT}/${label}.log" 2>&1 & SYNCER_PID=$!
  wait_for_syncer "${HTTP_PORT}"; sleep 1
}
stop_version() { stop_pid "${SYNCER_PID}"; SYNCER_PID=""; }

write_direct_conf() {
  local file=$1 src=$2 dst=$3
  cat >"${file}" <<EOF
server:
  listen: 127.0.0.1:${HTTP_PORT}
  listenPeer: 127.0.0.1:${HTTP_PORT}
  gracefullStopTimeout: 1s
input:
  redis:
    addresses: ["127.0.0.1:${src}"]
    type: standalone
    password: ${DATA_PASSWORD}
  mode: dynamic
  syncFrom: master
channel:
  storer:
    dirPath: ${TMP_ROOT}/store
    maxSize: 104857600
    logSize: 10485760
  staleCheckpointDuration: 10m
output:
  redis:
    addresses: ["127.0.0.1:${dst}"]
    type: standalone
    password: ${DATA_PASSWORD}
  replay:
    resumeFromBreakPoint: true
    keyExists: replace
    targetDb: -1
    replayTransaction: true
    bisyncEnabled: false
    mode: sync
log:
  level: info
  handler:
    stdout: true
  withCaller: false
  withFunc: false
EOF
}

write_sentinel_conf() {
  local file=$1 src_addrs dst_addrs
  src_addrs=$(format_addrs "${SRC_SENTINELS[@]}"); dst_addrs=$(format_addrs "${DST_SENTINELS[@]}")
  cat >"${file}" <<EOF
server:
  listen: 127.0.0.1:${HTTP_PORT}
  listenPeer: 127.0.0.1:${HTTP_PORT}
  gracefullStopTimeout: 1s
  checkRedisTypologyTicker: 1s
input:
  redis:
    addresses: [${src_addrs}]
    type: sentinel
    password: ${DATA_PASSWORD}
    sentinelOptions:
      masterName: ${SRC_NAME}
      password: ${SENTINEL_PASSWORD}
  mode: dynamic
  syncFrom: master
channel:
  storer:
    dirPath: ${TMP_ROOT}/store
    maxSize: 104857600
    logSize: 10485760
  staleCheckpointDuration: 10m
output:
  redis:
    addresses: [${dst_addrs}]
    type: sentinel
    password: ${DATA_PASSWORD}
    sentinelOptions:
      masterName: ${DST_NAME}
      password: ${SENTINEL_PASSWORD}
  replay:
    resumeFromBreakPoint: true
    keyExists: replace
    targetDb: -1
    replayTransaction: true
    bisyncEnabled: false
    mode: sync
log:
  level: info
  handler:
    stdout: true
  withCaller: false
  withFunc: false
EOF
}

build_nonbisync_binaries "${TMP_ROOT}"
start_data_group source "${SRC_MASTER}" "${SRC_REPLICA}"
start_data_group target "${DST_MASTER}" "${DST_REPLICA}"
start_sentinels source "${SRC_NAME}" "${SRC_MASTER}" "${SRC_SENTINELS[@]}"
start_sentinels target "${DST_NAME}" "${DST_MASTER}" "${DST_SENTINELS[@]}"

DIRECT_INITIAL="${TMP_ROOT}/previous-initial.yaml"; CURRENT_CONF="${TMP_ROOT}/current-sentinel.yaml"; DIRECT_ROLLBACK="${TMP_ROOT}/previous-rollback.yaml"
write_direct_conf "${DIRECT_INITIAL}" "${SRC_MASTER}" "${DST_MASTER}"
write_sentinel_conf "${CURRENT_CONF}"

start_version "${PREVIOUS_GUNYU_BIN}" "${DIRECT_INITIAL}" previous-initial
redis_data "${SRC_MASTER}" set sentinel:upgrade:before old >/dev/null
wait_value "${DST_MASTER}" sentinel:upgrade:before old
stop_version

redis_data "${SRC_MASTER}" set sentinel:upgrade:offline current >/dev/null
start_version "${TMP_ROOT}/redisGunYu" "${CURRENT_CONF}" current-sentinel
wait_value "${DST_MASTER}" sentinel:upgrade:offline current

redis_sentinel "${SRC_SENTINELS[0]}" sentinel failover "${SRC_NAME}" >/dev/null
NEW_SRC=$(wait_master_change "${SRC_SENTINELS[1]}" "${SRC_NAME}" "${SRC_MASTER}"); wait_data "${NEW_SRC}"
redis_sentinel "${DST_SENTINELS[0]}" sentinel failover "${DST_NAME}" >/dev/null
NEW_DST=$(wait_master_change "${DST_SENTINELS[1]}" "${DST_NAME}" "${DST_MASTER}"); wait_data "${NEW_DST}"
redis_data "${NEW_SRC}" set sentinel:upgrade:current sentinel >/dev/null
wait_value "${NEW_DST}" sentinel:upgrade:current sentinel
stop_version

redis_data "${NEW_SRC}" set sentinel:upgrade:rollback-offline previous >/dev/null
write_direct_conf "${DIRECT_ROLLBACK}" "${NEW_SRC}" "${NEW_DST}"
start_version "${PREVIOUS_GUNYU_BIN}" "${DIRECT_ROLLBACK}" previous-rollback
wait_value "${NEW_DST}" sentinel:upgrade:rollback-offline previous

for suffix in before offline current rollback-offline; do
  expect_eq "$(redis_data "${NEW_DST}" get "sentinel:upgrade:${suffix}")" "$(redis_data "${NEW_SRC}" get "sentinel:upgrade:${suffix}")" "upgrade/rollback ${suffix}"
done
CHECKPOINTS=$(redis_data "${NEW_DST}" hlen redis-gunyu-checkpoint-hash)
BISYNC=$(redis_data "${NEW_DST}" --scan --pattern 'redis-gunyu-bisync:*' | wc -l | tr -d '[:space:]')
if ((CHECKPOINTS < 1)) || [[ "${BISYNC}" != "0" ]]; then echo "metadata invariant failed" >&2; exit 1; fi
cat >"${TMP_ROOT}/result.txt" <<EOF
previous_binary=${PREVIOUS_GUNYU_BIN}
current_binary=${TMP_ROOT}/redisGunYu
source_master_after=${NEW_SRC}
target_master_after=${NEW_DST}
business_keys=4
checkpoint_entries=${CHECKPOINTS}
bisync_metadata=${BISYNC}
status=PASS
EOF
echo "sentinel_upgrade_rollback_result=${TMP_ROOT}/result.txt"
