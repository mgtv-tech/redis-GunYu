#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/nonbisync/lib/test_env.sh"
require_test_commands go redis-server redis-cli curl

TMP_ROOT="${TMPDIR:-/tmp}/redisgunyu-sentinel-e2e"
REDIS_SERVER_BIN="$(resolve_redis_server_bin REDIS_SERVER_BIN REDIS_DEPLOY_ROOT)"
PORT_BASE="${SENTINEL_PORT_BASE:-32900}"
SRC_MASTER_PORT=$((PORT_BASE + 0))
SRC_REPLICA_PORT=$((PORT_BASE + 1))
SRC_SENTINEL_PORTS=("$((PORT_BASE + 2))" "$((PORT_BASE + 3))" "$((PORT_BASE + 4))")
DST_MASTER_PORT=$((PORT_BASE + 5))
DST_REPLICA_PORT=$((PORT_BASE + 6))
DST_SENTINEL_PORTS=("$((PORT_BASE + 7))" "$((PORT_BASE + 8))" "$((PORT_BASE + 9))")
HTTP_PORT=$((PORT_BASE + 10))
REPLAY_MODE="${SENTINEL_REPLAY_MODE:-sync}"
DATA_PASSWORD="gunyu-data-password"
SENTINEL_PASSWORD="gunyu-sentinel-password"
SRC_MASTER_NAME="gunyu-source"
DST_MASTER_NAME="gunyu-target"
SYNCER_PID=""

stop_owned_processes() {
  local pid_file pid
  while IFS= read -r pid_file; do
    [[ -f "${pid_file}" ]] || continue
    pid=$(tr -d '[:space:]' < "${pid_file}")
    if [[ "${pid}" =~ ^[0-9]+$ ]] && kill -0 "${pid}" >/dev/null 2>&1; then
      kill "${pid}" >/dev/null 2>&1 || true
    fi
  done < <(find "${TMP_ROOT}" -type f -name redis.pid 2>/dev/null)
}

write_diagnostics() {
  local port
  {
    echo "timestamp=$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    for port in "${SRC_SENTINEL_PORTS[@]}"; do
      echo "source_sentinel=${port}"
      redis_auth "${SENTINEL_PASSWORD}" "${port}" sentinel master "${SRC_MASTER_NAME}" 2>&1 || true
    done
    for port in "${DST_SENTINEL_PORTS[@]}"; do
      echo "target_sentinel=${port}"
      redis_auth "${SENTINEL_PASSWORD}" "${port}" sentinel master "${DST_MASTER_NAME}" 2>&1 || true
    done
    for port in "${SRC_MASTER_PORT}" "${SRC_REPLICA_PORT}" "${DST_MASTER_PORT}" "${DST_REPLICA_PORT}"; do
      echo "redis_replication=${port}"
      redis_auth "${DATA_PASSWORD}" "${port}" info replication 2>&1 || true
    done
  } >"${TMP_ROOT}/diagnostics.txt"
}

cleanup() {
  local code=$?
  set +e
  if [[ ${code} -ne 0 && -d "${TMP_ROOT}" ]]; then
    write_diagnostics
  fi
  stop_pid "${SYNCER_PID:-}"
  stop_owned_processes
  if [[ "${KEEP_TMP:-0}" != "1" && -n "${TMP_ROOT}" && "${TMP_ROOT}" == */redisgunyu-sentinel-e2e ]]; then
    rm -rf -- "${TMP_ROOT}"
  fi
  exit "${code}"
}
trap cleanup EXIT

rm -rf -- "${TMP_ROOT}"
mkdir -p "${TMP_ROOT}"

redis_auth() {
  local password=$1
  local port=$2
  shift 2
  REDISCLI_AUTH="${password}" redis-cli -p "${port}" --raw "$@"
}

wait_for_auth_ping() {
  local password=$1
  local port=$2
  for _ in $(seq 1 100); do
    if redis_auth "${password}" "${port}" ping 2>/dev/null | match_regex_quiet '^PONG$'; then
      return 0
    fi
    sleep 0.1
  done
  echo "authenticated Redis on port ${port} did not start" >&2
  return 1
}

wait_for_replica_up() {
  local port=$1
  for _ in $(seq 1 100); do
    if redis_auth "${DATA_PASSWORD}" "${port}" info replication 2>/dev/null | match_regex_quiet '^master_link_status:up'; then
      return 0
    fi
    sleep 0.1
  done
  echo "replica on port ${port} did not connect to its master" >&2
  return 1
}

sentinel_master_field() {
  local field=$1
  awk -v field="${field}" '$0 == field { if (getline > 0) print; exit }'
}

wait_for_sentinel_group_ready() {
  local master_name=$1
  local expected_master_port=$2
  shift 2
  local port info reported_port flags replicas peers ready

  for _ in $(seq 1 300); do
    ready=1
    for port in "$@"; do
      info=$(redis_auth "${SENTINEL_PASSWORD}" "${port}" sentinel master "${master_name}" 2>/dev/null || true)
      reported_port=$(printf '%s\n' "${info}" | sentinel_master_field port)
      flags=$(printf '%s\n' "${info}" | sentinel_master_field flags)
      replicas=$(printf '%s\n' "${info}" | sentinel_master_field num-slaves)
      peers=$(printf '%s\n' "${info}" | sentinel_master_field num-other-sentinels)
      if [[ "${reported_port}" != "${expected_master_port}" ||
            "${flags}" == *s_down* || "${flags}" == *o_down* ||
            ! "${replicas}" =~ ^[0-9]+$ || "${replicas}" -lt 1 ||
            ! "${peers}" =~ ^[0-9]+$ || "${peers}" -lt 2 ]]; then
        ready=0
        break
      fi
    done
    if [[ ${ready} -eq 1 ]]; then
      return 0
    fi
    sleep 0.1
  done
  echo "Sentinel group ${master_name} did not discover its replica and peers" >&2
  return 1
}

start_data_group() {
  local prefix=$1
  local master_port=$2
  local replica_port=$3
  local master_dir="${TMP_ROOT}/${prefix}-master-${master_port}"
  local replica_dir="${TMP_ROOT}/${prefix}-replica-${replica_port}"

  write_standalone_conf "${master_dir}" "${master_port}" "requirepass ${DATA_PASSWORD}
masterauth ${DATA_PASSWORD}"
  "${REDIS_SERVER_BIN}" "${master_dir}/redis.conf"
  wait_for_auth_ping "${DATA_PASSWORD}" "${master_port}"

  write_standalone_conf "${replica_dir}" "${replica_port}" "requirepass ${DATA_PASSWORD}
masterauth ${DATA_PASSWORD}
replicaof 127.0.0.1 ${master_port}"
  "${REDIS_SERVER_BIN}" "${replica_dir}/redis.conf"
  wait_for_auth_ping "${DATA_PASSWORD}" "${replica_port}"
  wait_for_replica_up "${replica_port}"
}

start_sentinel_group() {
  local prefix=$1
  local master_name=$2
  local master_port=$3
  shift 3
  local port dir
  for port in "$@"; do
    dir="${TMP_ROOT}/${prefix}-sentinel-${port}"
    mkdir -p "${dir}"
    cat > "${dir}/sentinel.conf" <<EOF
port ${port}
bind 127.0.0.1
protected-mode no
daemonize yes
dir ${dir}
pidfile ${dir}/redis.pid
logfile ${dir}/redis.log
requirepass ${SENTINEL_PASSWORD}
sentinel monitor ${master_name} 127.0.0.1 ${master_port} 2
sentinel auth-pass ${master_name} ${DATA_PASSWORD}
sentinel down-after-milliseconds ${master_name} 500
sentinel failover-timeout ${master_name} 5000
sentinel parallel-syncs ${master_name} 1
EOF
    "${REDIS_SERVER_BIN}" "${dir}/sentinel.conf" --sentinel
    wait_for_auth_ping "${SENTINEL_PASSWORD}" "${port}"
  done
  wait_for_sentinel_group_ready "${master_name}" "${master_port}" "$@"
}

sentinel_master_port() {
  local sentinel_port=$1
  local master_name=$2
  redis_auth "${SENTINEL_PASSWORD}" "${sentinel_port}" sentinel get-master-addr-by-name "${master_name}" | tail -n 1
}

wait_for_master_change() {
  local master_name=$1
  local old_port=$2
  shift 2
  local port current candidate role ready
  for _ in $(seq 1 300); do
    candidate=""
    ready=1
    for port in "$@"; do
      current=$(sentinel_master_port "${port}" "${master_name}" 2>/dev/null || true)
      if [[ -z "${current}" || "${current}" == "${old_port}" ]]; then
        ready=0
        break
      fi
      if [[ -z "${candidate}" ]]; then
        candidate="${current}"
      elif [[ "${candidate}" != "${current}" ]]; then
        ready=0
        break
      fi
    done
    if [[ ${ready} -eq 1 ]]; then
      role=$(redis_auth "${DATA_PASSWORD}" "${candidate}" info replication 2>/dev/null | sed -n 's/^role:\([^[:space:]]*\).*/\1/p' | tr -d '\r' || true)
      if [[ "${role}" == "master" ]]; then
        printf '%s\n' "${candidate}"
        return 0
      fi
    fi
    sleep 0.1
  done
  echo "Sentinel group ${master_name} did not fail over from ${old_port}" >&2
  return 1
}

wait_for_value() {
  local port=$1
  local key=$2
  local expected=$3
  local actual
  for _ in $(seq 1 200); do
    actual=$(redis_auth "${DATA_PASSWORD}" "${port}" get "${key}" 2>/dev/null || true)
    if [[ "${actual}" == "${expected}" ]]; then
      return 0
    fi
    sleep 0.1
  done
  echo "key ${key} on port ${port}: expected ${expected}, got ${actual:-<empty>}" >&2
  return 1
}

echo "[1/5] building redisGunYu"
build_nonbisync_binaries "${TMP_ROOT}"

echo "[2/5] starting source and target Sentinel groups"
start_data_group source "${SRC_MASTER_PORT}" "${SRC_REPLICA_PORT}"
start_data_group target "${DST_MASTER_PORT}" "${DST_REPLICA_PORT}"
start_sentinel_group source "${SRC_MASTER_NAME}" "${SRC_MASTER_PORT}" "${SRC_SENTINEL_PORTS[@]}"
start_sentinel_group target "${DST_MASTER_NAME}" "${DST_MASTER_PORT}" "${DST_SENTINEL_PORTS[@]}"

SRC_SENTINEL_ADDRS=$(format_addrs "${SRC_SENTINEL_PORTS[@]}")
DST_SENTINEL_ADDRS=$(format_addrs "${DST_SENTINEL_PORTS[@]}")
CONF_FILE="${TMP_ROOT}/sentinel.yaml"
cat > "${CONF_FILE}" <<EOF
server:
  listen: 127.0.0.1:${HTTP_PORT}
  listenPeer: 127.0.0.1:${HTTP_PORT}
  gracefullStopTimeout: 1s
  checkRedisTypologyTicker: 1s
input:
  redis:
    addresses: [${SRC_SENTINEL_ADDRS}]
    type: sentinel
    password: ${DATA_PASSWORD}
    sentinelOptions:
      masterName: ${SRC_MASTER_NAME}
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
    addresses: [${DST_SENTINEL_ADDRS}]
    type: sentinel
    password: ${DATA_PASSWORD}
    sentinelOptions:
      masterName: ${DST_MASTER_NAME}
      password: ${SENTINEL_PASSWORD}
  replay:
    resumeFromBreakPoint: true
    keyExists: replace
    targetDb: -1
    replayTransaction: true
    bisyncEnabled: false
    mode: ${REPLAY_MODE}
log:
  level: info
  handler:
    stdout: true
  withCaller: false
  withFunc: false
EOF

SYNCER_PID=$(start_syncer_process "${TMP_ROOT}" "${CONF_FILE}" "${TMP_ROOT}/syncer.log")
wait_for_syncer "${HTTP_PORT}"
sleep 2

echo "[3/5] verifying initial synchronization and source failover"
redis_auth "${DATA_PASSWORD}" "${SRC_MASTER_PORT}" set sentinel:e2e:value before-failover >/dev/null
redis_auth "${DATA_PASSWORD}" "${SRC_MASTER_PORT}" incrby sentinel:e2e:counter 3 >/dev/null
redis_auth "${DATA_PASSWORD}" "${SRC_MASTER_PORT}" wait 1 5000 >/dev/null
wait_for_value "${DST_MASTER_PORT}" sentinel:e2e:counter 3

redis_auth "${SENTINEL_PASSWORD}" "${SRC_SENTINEL_PORTS[0]}" sentinel failover "${SRC_MASTER_NAME}" >/dev/null
NEW_SRC_MASTER_PORT=$(wait_for_master_change "${SRC_MASTER_NAME}" "${SRC_MASTER_PORT}" "${SRC_SENTINEL_PORTS[@]}")
wait_for_auth_ping "${DATA_PASSWORD}" "${NEW_SRC_MASTER_PORT}"
redis_auth "${DATA_PASSWORD}" "${NEW_SRC_MASTER_PORT}" set sentinel:e2e:value after-source-failover >/dev/null
redis_auth "${DATA_PASSWORD}" "${NEW_SRC_MASTER_PORT}" incrby sentinel:e2e:counter 4 >/dev/null
wait_for_value "${DST_MASTER_PORT}" sentinel:e2e:counter 7
wait_for_value "${DST_MASTER_PORT}" sentinel:e2e:value after-source-failover

echo "[4/5] verifying target failover and checkpointed replay"
redis_auth "${DATA_PASSWORD}" "${DST_MASTER_PORT}" wait 1 5000 >/dev/null
redis_auth "${SENTINEL_PASSWORD}" "${DST_SENTINEL_PORTS[0]}" sentinel failover "${DST_MASTER_NAME}" >/dev/null
NEW_DST_MASTER_PORT=$(wait_for_master_change "${DST_MASTER_NAME}" "${DST_MASTER_PORT}" "${DST_SENTINEL_PORTS[@]}")
wait_for_auth_ping "${DATA_PASSWORD}" "${NEW_DST_MASTER_PORT}"
redis_auth "${DATA_PASSWORD}" "${NEW_SRC_MASTER_PORT}" incrby sentinel:e2e:counter 5 >/dev/null
redis_auth "${DATA_PASSWORD}" "${NEW_SRC_MASTER_PORT}" set sentinel:e2e:value after-target-failover >/dev/null
wait_for_value "${NEW_DST_MASTER_PORT}" sentinel:e2e:counter 12
wait_for_value "${NEW_DST_MASTER_PORT}" sentinel:e2e:value after-target-failover

echo "[5/5] checking checkpoint and metadata invariants"
CHECKPOINT_COUNT=$(redis_auth "${DATA_PASSWORD}" "${NEW_DST_MASTER_PORT}" hlen redis-gunyu-checkpoint-hash)
if [[ "${CHECKPOINT_COUNT}" -lt 1 ]]; then
  echo "checkpoint hash is empty after Sentinel failovers" >&2
  exit 1
fi
BISYNC_COUNT=$(redis_auth "${DATA_PASSWORD}" "${NEW_DST_MASTER_PORT}" --scan --pattern 'redis-gunyu-bisync:*' | wc -l | tr -d '[:space:]')
if [[ "${BISYNC_COUNT}" != "0" ]]; then
  echo "non-bisync Sentinel run created bisync metadata" >&2
  exit 1
fi

{
  echo "source_master_before=${SRC_MASTER_PORT}"
  echo "source_master_after=${NEW_SRC_MASTER_PORT}"
  echo "target_master_before=${DST_MASTER_PORT}"
  echo "target_master_after=${NEW_DST_MASTER_PORT}"
  echo "counter=12"
  echo "checkpoint_entries=${CHECKPOINT_COUNT}"
  echo "bisync_metadata=${BISYNC_COUNT}"
	echo "replay_mode=${REPLAY_MODE}"
} > "${TMP_ROOT}/result.txt"

echo "sentinel_e2e_result=${TMP_ROOT}/result.txt"
