#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/nonbisync/lib/test_env.sh"
require_test_commands go redis-server redis-cli curl openssl

PORT_BASE="${SENTINEL_SECURITY_PORT_BASE:-42000}"
DATA_TLS="${SENTINEL_DATA_TLS:-0}"
SENTINEL_TLS="${SENTINEL_TLS:-0}"
CASE_NAME="${SENTINEL_SECURITY_CASE:-acl}"
TMP_ROOT="${TMPDIR:-/tmp}/redisgunyu-sentinel-security-${CASE_NAME}"
REDIS_SERVER_BIN="$(resolve_redis_server_bin REDIS_SERVER_BIN REDIS_DEPLOY_ROOT)"
DATA_USER="gunyu-data-user"
DATA_PASSWORD="gunyu-data-password"
SENTINEL_USER="gunyu-sentinel-user"
SENTINEL_PASSWORD="gunyu-sentinel-password"
SRC_NAME="gunyu-security-source"
DST_NAME="gunyu-security-target"
SRC_MASTER=$((PORT_BASE + 0))
SRC_REPLICA=$((PORT_BASE + 1))
SRC_SENTINELS=("$((PORT_BASE + 2))" "$((PORT_BASE + 3))" "$((PORT_BASE + 4))")
DST_MASTER=$((PORT_BASE + 5))
DST_REPLICA=$((PORT_BASE + 6))
DST_SENTINELS=("$((PORT_BASE + 7))" "$((PORT_BASE + 8))" "$((PORT_BASE + 9))")
HTTP_PORT=$((PORT_BASE + 10))
SYNCER_PID=""
CERT_DIR="${TMP_ROOT}/tls"

stop_owned_processes() {
  local pid_file pid
  while IFS= read -r pid_file; do
    [[ -f "${pid_file}" ]] || continue
    pid=$(tr -d '[:space:]' <"${pid_file}")
    if [[ "${pid}" =~ ^[0-9]+$ ]] && kill -0 "${pid}" >/dev/null 2>&1; then
      kill "${pid}" >/dev/null 2>&1 || true
    fi
  done < <(find "${TMP_ROOT}" -type f -name redis.pid 2>/dev/null)
}

cleanup() {
  local code=$?
  set +e
  stop_pid "${SYNCER_PID:-}"
  stop_owned_processes
  exit "${code}"
}
trap cleanup EXIT

mkdir -p "${TMP_ROOT}" "${CERT_DIR}"

openssl req -x509 -newkey rsa:2048 -sha256 -days 1 -nodes \
  -keyout "${CERT_DIR}/ca.key" -out "${CERT_DIR}/ca.crt" -subj "/CN=sentinel-security-ca" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes \
  -keyout "${CERT_DIR}/server.key" -out "${CERT_DIR}/server.csr" -subj "/CN=127.0.0.1" >/dev/null 2>&1
openssl x509 -req -in "${CERT_DIR}/server.csr" -CA "${CERT_DIR}/ca.crt" -CAkey "${CERT_DIR}/ca.key" -CAcreateserial \
  -out "${CERT_DIR}/server.crt" -days 1 -sha256 -extfile <(printf 'subjectAltName=IP:127.0.0.1\n') >/dev/null 2>&1

tls_server_conf() {
  cat <<EOF
tls-cert-file ${CERT_DIR}/server.crt
tls-key-file ${CERT_DIR}/server.key
tls-ca-cert-file ${CERT_DIR}/ca.crt
tls-auth-clients no
EOF
}

data_cli() {
  local port=$1
  shift
  local args=(--user "${DATA_USER}" -p "${port}" --raw)
  if [[ "${DATA_TLS}" == "1" ]]; then
    args+=(--tls --cacert "${CERT_DIR}/ca.crt")
  fi
  REDISCLI_AUTH="${DATA_PASSWORD}" redis-cli "${args[@]}" "$@"
}

sentinel_cli() {
  local port=$1
  shift
  local args=(--user "${SENTINEL_USER}" -p "${port}" --raw)
  if [[ "${SENTINEL_TLS}" == "1" ]]; then
    args+=(--tls --cacert "${CERT_DIR}/ca.crt")
  fi
  REDISCLI_AUTH="${SENTINEL_PASSWORD}" redis-cli "${args[@]}" "$@"
}

wait_data_ping() {
  local port=$1
  for _ in $(seq 1 100); do
    if data_cli "${port}" ping 2>/dev/null | match_regex_quiet '^PONG$'; then return 0; fi
    sleep 0.1
  done
  echo "data node ${port} did not start" >&2
  return 1
}

wait_sentinel_ping() {
  local port=$1
  for _ in $(seq 1 100); do
    if sentinel_cli "${port}" ping 2>/dev/null | match_regex_quiet '^PONG$'; then return 0; fi
    sleep 0.1
  done
  echo "sentinel ${port} did not start" >&2
  return 1
}

wait_replica_up() {
  local port=$1
  for _ in $(seq 1 150); do
    if data_cli "${port}" info replication 2>/dev/null | match_regex_quiet '^master_link_status:up'; then return 0; fi
    sleep 0.1
  done
  echo "replica ${port} did not connect" >&2
  return 1
}

write_data_conf() {
  local dir=$1 port=$2 replicaof=${3:-}
  mkdir -p "${dir}"
  {
    echo "bind 127.0.0.1"
    echo "protected-mode no"
    echo "daemonize yes"
    echo "dir ${dir}"
    echo "pidfile ${dir}/redis.pid"
    echo "logfile ${dir}/redis.log"
    echo "save \"\""
    echo "appendonly no"
    if [[ "${DATA_TLS}" == "1" ]]; then
      echo "port 0"
      echo "tls-port ${port}"
      tls_server_conf
      echo "tls-replication yes"
    else
      echo "port ${port}"
    fi
    echo "user default off"
    echo "user ${DATA_USER} on >${DATA_PASSWORD} ~* &* +@all"
    echo "masteruser ${DATA_USER}"
    echo "masterauth ${DATA_PASSWORD}"
    if [[ -n "${replicaof}" ]]; then echo "replicaof 127.0.0.1 ${replicaof}"; fi
  } >"${dir}/redis.conf"
}

start_data_group() {
  local prefix=$1 master=$2 replica=$3
  write_data_conf "${TMP_ROOT}/${prefix}-master-${master}" "${master}"
  "${REDIS_SERVER_BIN}" "${TMP_ROOT}/${prefix}-master-${master}/redis.conf"
  wait_data_ping "${master}"
  write_data_conf "${TMP_ROOT}/${prefix}-replica-${replica}" "${replica}" "${master}"
  "${REDIS_SERVER_BIN}" "${TMP_ROOT}/${prefix}-replica-${replica}/redis.conf"
  wait_data_ping "${replica}"
  wait_replica_up "${replica}"
}

start_sentinel_group() {
  local prefix=$1 master_name=$2 master_port=$3
  shift 3
  local port dir
  for port in "$@"; do
    dir="${TMP_ROOT}/${prefix}-sentinel-${port}"
    mkdir -p "${dir}"
    {
      echo "bind 127.0.0.1"
      echo "protected-mode no"
      echo "daemonize yes"
      echo "dir ${dir}"
      echo "pidfile ${dir}/redis.pid"
      echo "logfile ${dir}/redis.log"
      if [[ "${SENTINEL_TLS}" == "1" ]]; then
        echo "port 0"
        echo "tls-port ${port}"
        tls_server_conf
        echo "sentinel announce-ip 127.0.0.1"
        echo "sentinel announce-port ${port}"
      else
        echo "port ${port}"
        if [[ "${DATA_TLS}" == "1" ]]; then tls_server_conf; fi
      fi
      if [[ "${DATA_TLS}" == "1" ]]; then echo "tls-replication yes"; fi
      echo "user default off"
      echo "user ${SENTINEL_USER} on >${SENTINEL_PASSWORD} ~* +@all"
      echo "sentinel monitor ${master_name} 127.0.0.1 ${master_port} 2"
      echo "sentinel auth-user ${master_name} ${DATA_USER}"
      echo "sentinel auth-pass ${master_name} ${DATA_PASSWORD}"
      echo "sentinel down-after-milliseconds ${master_name} 500"
      echo "sentinel failover-timeout ${master_name} 5000"
      echo "sentinel parallel-syncs ${master_name} 1"
    } >"${dir}/sentinel.conf"
    "${REDIS_SERVER_BIN}" "${dir}/sentinel.conf" --sentinel
    wait_sentinel_ping "${port}"
  done
}

sentinel_master_port() {
  sentinel_cli "$1" sentinel get-master-addr-by-name "$2" | tail -n 1
}

wait_master_change() {
  local sentinel_port=$1 master_name=$2 old_port=$3 current
  for _ in $(seq 1 250); do
    current=$(sentinel_master_port "${sentinel_port}" "${master_name}" 2>/dev/null || true)
    if [[ -n "${current}" && "${current}" != "${old_port}" ]]; then printf '%s\n' "${current}"; return 0; fi
    sleep 0.1
  done
  echo "${master_name} did not fail over" >&2
  return 1
}

wait_value() {
  local port=$1 key=$2 expected=$3 actual
  for _ in $(seq 1 200); do
    actual=$(data_cli "${port}" get "${key}" 2>/dev/null || true)
    if [[ "${actual}" == "${expected}" ]]; then return 0; fi
    sleep 0.1
  done
  echo "${key} on ${port}: expected ${expected}, got ${actual:-empty}" >&2
  return 1
}

build_nonbisync_binaries "${TMP_ROOT}"
start_data_group source "${SRC_MASTER}" "${SRC_REPLICA}"
start_data_group target "${DST_MASTER}" "${DST_REPLICA}"
start_sentinel_group source "${SRC_NAME}" "${SRC_MASTER}" "${SRC_SENTINELS[@]}"
start_sentinel_group target "${DST_NAME}" "${DST_MASTER}" "${DST_SENTINELS[@]}"

SRC_ADDRS=$(format_addrs "${SRC_SENTINELS[@]}")
DST_ADDRS=$(format_addrs "${DST_SENTINELS[@]}")
CONF="${TMP_ROOT}/syncer.yaml"
cat >"${CONF}" <<EOF
server:
  listen: 127.0.0.1:${HTTP_PORT}
  listenPeer: 127.0.0.1:${HTTP_PORT}
  gracefullStopTimeout: 1s
  checkRedisTypologyTicker: 1s
input:
  redis:
    addresses: [${SRC_ADDRS}]
    type: sentinel
    userName: ${DATA_USER}
    password: ${DATA_PASSWORD}
    tlsEnable: $([[ "${DATA_TLS}" == "1" ]] && echo true || echo false)
    sentinelOptions:
      masterName: ${SRC_NAME}
      userName: ${SENTINEL_USER}
      password: ${SENTINEL_PASSWORD}
      tlsEnable: $([[ "${SENTINEL_TLS}" == "1" ]] && echo true || echo false)
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
    addresses: [${DST_ADDRS}]
    type: sentinel
    userName: ${DATA_USER}
    password: ${DATA_PASSWORD}
    tlsEnable: $([[ "${DATA_TLS}" == "1" ]] && echo true || echo false)
    sentinelOptions:
      masterName: ${DST_NAME}
      userName: ${SENTINEL_USER}
      password: ${SENTINEL_PASSWORD}
      tlsEnable: $([[ "${SENTINEL_TLS}" == "1" ]] && echo true || echo false)
  replay:
    resumeFromBreakPoint: true
    keyExists: replace
    targetDb: -1
    replayTransaction: true
    bisyncEnabled: false
    mode: pipeline
log:
  level: info
  handler:
    stdout: true
  withCaller: false
  withFunc: false
EOF

SYNCER_PID=$(start_syncer_process "${TMP_ROOT}" "${CONF}" "${TMP_ROOT}/syncer.log")
wait_for_syncer "${HTTP_PORT}"
data_cli "${SRC_MASTER}" set "sentinel:security:${CASE_NAME}" before-failover >/dev/null
wait_value "${DST_MASTER}" "sentinel:security:${CASE_NAME}" before-failover

sentinel_cli "${SRC_SENTINELS[0]}" sentinel failover "${SRC_NAME}" >/dev/null
NEW_SRC=$(wait_master_change "${SRC_SENTINELS[1]}" "${SRC_NAME}" "${SRC_MASTER}")
wait_data_ping "${NEW_SRC}"
data_cli "${NEW_SRC}" set "sentinel:security:${CASE_NAME}" after-source-failover >/dev/null
wait_value "${DST_MASTER}" "sentinel:security:${CASE_NAME}" after-source-failover

sentinel_cli "${DST_SENTINELS[0]}" sentinel failover "${DST_NAME}" >/dev/null
NEW_DST=$(wait_master_change "${DST_SENTINELS[1]}" "${DST_NAME}" "${DST_MASTER}")
wait_data_ping "${NEW_DST}"
data_cli "${NEW_SRC}" set "sentinel:security:${CASE_NAME}" after-target-failover >/dev/null
wait_value "${NEW_DST}" "sentinel:security:${CASE_NAME}" after-target-failover

CHECKPOINTS=$(data_cli "${NEW_DST}" hlen redis-gunyu-checkpoint-hash)
BISYNC=$(data_cli "${NEW_DST}" --scan --pattern 'redis-gunyu-bisync:*' | wc -l | tr -d '[:space:]')
if ((CHECKPOINTS < 1)) || [[ "${BISYNC}" != "0" ]]; then
  echo "metadata invariant failed: checkpoints=${CHECKPOINTS}, bisync=${BISYNC}" >&2
  exit 1
fi

cat >"${TMP_ROOT}/result.txt" <<EOF
case=${CASE_NAME}
data_tls=${DATA_TLS}
sentinel_tls=${SENTINEL_TLS}
source_master_after=${NEW_SRC}
target_master_after=${NEW_DST}
value=after-target-failover
checkpoint_entries=${CHECKPOINTS}
bisync_metadata=${BISYNC}
EOF
echo "sentinel_security_result=${TMP_ROOT}/result.txt"
