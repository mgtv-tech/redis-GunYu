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
HTTP_PORT_B=$((PORT_BASE + 11))
REPLAY_MODE="${SENTINEL_REPLAY_MODE:-sync}"
DATA_PASSWORD="gunyu-data-password"
SENTINEL_PASSWORD="gunyu-sentinel-password"
SRC_MASTER_NAME="gunyu-source"
DST_MASTER_NAME="gunyu-target"
SYNCER_PID=""
SYNCER_PID_A=""
SYNCER_PID_B=""
WRITER_PID=""

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
    echo "accepted_writes=$(accepted_count)"
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
    for port in "${HTTP_PORT}" "${HTTP_PORT_B}"; do
      echo "syncer_status=${port}"
      curl -sf "http://127.0.0.1:${port}/syncer/status" 2>&1 || true
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
  stop_pid "${SYNCER_PID_A:-}"
  stop_pid "${SYNCER_PID_B:-}"
  stop_pid "${WRITER_PID:-}"
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

accepted_count() {
  if [[ ! -f "${TMP_ROOT}/accepted-sequences.txt" ]]; then
    printf '0\n'
    return 0
  fi
  wc -l <"${TMP_ROOT}/accepted-sequences.txt" | tr -d '[:space:]'
}

wait_for_accepted_count() {
  local minimum=$1
  local timeout_seconds=$2
  local count
  for _ in $(seq 1 $((timeout_seconds * 5))); do
    count=$(accepted_count)
    if ((count >= minimum)); then
      return 0
    fi
    if [[ -n "${WRITER_PID}" ]] && ! kill -0 "${WRITER_PID}" >/dev/null 2>&1; then
      echo "writer exited after ${count} acknowledged writes; expected at least ${minimum}" >&2
      return 1
    fi
    sleep 0.2
  done
  echo "timed out after ${timeout_seconds}s with $(accepted_count) acknowledged writes; expected at least ${minimum}" >&2
  return 1
}

wait_for_sequence_convergence() {
  local expected=$1
  local timeout_seconds=$2
  for _ in $(seq 1 $((timeout_seconds * 2))); do
    src_count=$(redis_auth "${DATA_PASSWORD}" "${new_master}" --scan --pattern 'sentinel:ha:seq:*' 2>/dev/null | wc -l | tr -d '[:space:]')
    dst_count=$(redis_auth "${DATA_PASSWORD}" "${dst_master}" --scan --pattern 'sentinel:ha:seq:*' 2>/dev/null | wc -l | tr -d '[:space:]')
    if ((src_count >= expected && dst_count == src_count)); then
      return 0
    fi
    sleep 0.5
  done
  echo "sequence keys did not converge after ${timeout_seconds}s: acknowledged=${expected}, source=${src_count:-0}, target=${dst_count:-0}" >&2
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
  local sentinel_port=$1
  local master_name=$2
  local old_port=$3
  local current
  for _ in $(seq 1 200); do
    current=$(sentinel_master_port "${sentinel_port}" "${master_name}" 2>/dev/null || true)
    if [[ -n "${current}" && "${current}" != "${old_port}" ]]; then
      printf '%s\n' "${current}"
      return 0
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
  initialPaused: true
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
cluster:
  groupName: sentinel-ha-competition
  leaseTimeout: 6s
EOF

run_competition() {
  local conf_a="${TMP_ROOT}/sentinel-a.yaml" conf_b="${TMP_ROOT}/sentinel-b.yaml"
  local before_takeover minimum_after_takeover
  cp "${CONF_FILE}" "${conf_a}"
  sed "s/127.0.0.1:${HTTP_PORT}/127.0.0.1:${HTTP_PORT_B}/g; s#${TMP_ROOT}/store#${TMP_ROOT}/store-b#g" "${CONF_FILE}" >"${conf_b}"
  : >"${TMP_ROOT}/accepted-sequences.txt"
  (
    seq=0
    while [[ ! -f "${TMP_ROOT}/stop-writer" ]]; do
      current=$(sentinel_master_port "${SRC_SENTINEL_PORTS[0]}" "${SRC_MASTER_NAME}" 2>/dev/null || true)
      if [[ -n "${current}" ]]; then
        replies=$(printf 'SET sentinel:ha:seq:%s %s\nWAIT 1 2000\n' "${seq}" "${seq}" | REDISCLI_AUTH="${DATA_PASSWORD}" redis-cli -p "${current}" --raw 2>/dev/null || true)
        set_reply=$(printf '%s\n' "${replies}" | sed -n '1p')
        ack=$(printf '%s\n' "${replies}" | sed -n '2p')
        if [[ "${set_reply}" == "OK" && "${ack}" =~ ^[1-9][0-9]*$ ]]; then
          printf '%s\n' "${seq}" >>"${TMP_ROOT}/accepted-sequences.txt"
          seq=$((seq + 1))
        fi
      fi
      sleep 0.02
    done
  ) >"${TMP_ROOT}/writer.log" 2>&1 &
  WRITER_PID=$!
  SYNCER_PID_A=$(start_syncer_process "${TMP_ROOT}" "${conf_a}" "${TMP_ROOT}/syncer-a.log")
  wait_for_syncer "${HTTP_PORT}"
  wait_for_log_pattern "${TMP_ROOT}/syncer-a.log" 'new_role\(leader\)|RunLeader' 30
  SYNCER_PID_B=$(start_syncer_process "${TMP_ROOT}" "${conf_b}" "${TMP_ROOT}/syncer-b.log")
  wait_for_syncer "${HTTP_PORT_B}"
  wait_for_log_pattern "${TMP_ROOT}/syncer-b.log" 'new_role\(follower\)|RunFollower' 30
  curl -sf -XPOST "http://127.0.0.1:${HTTP_PORT}/syncer/resume?inputs=all" >/dev/null
  curl -sf -XPOST "http://127.0.0.1:${HTTP_PORT_B}/syncer/resume?inputs=all" >/dev/null
  wait_for_accepted_count 20 45
  before_takeover=$(accepted_count)
  stop_pid "${SYNCER_PID_A}"
  SYNCER_PID_A=""
  wait_for_log_pattern "${TMP_ROOT}/syncer-b.log" 'new_role\(leader\)|RunLeader' 40
  old_master=$(sentinel_master_port "${SRC_SENTINEL_PORTS[0]}" "${SRC_MASTER_NAME}")
  new_master="${old_master}"
  minimum_after_takeover=$((before_takeover + 20))
  wait_for_accepted_count "${minimum_after_takeover}" 45
  touch "${TMP_ROOT}/stop-writer"
  wait_for_pid_exit "${WRITER_PID}" 10
  wait "${WRITER_PID}" >/dev/null 2>&1 || true
  WRITER_PID=""
  dst_master=$(sentinel_master_port "${DST_SENTINEL_PORTS[1]}" "${DST_MASTER_NAME}")
  acked=$(accepted_count)
  wait_for_sequence_convergence "${acked}" 60
  while IFS= read -r seq; do
    expect_eq "$(redis_auth "${DATA_PASSWORD}" "${new_master}" get "sentinel:ha:seq:${seq}")" "${seq}" "source sequence ${seq}"
    expect_eq "$(redis_auth "${DATA_PASSWORD}" "${dst_master}" get "sentinel:ha:seq:${seq}")" "${seq}" "target sequence ${seq}"
  done <"${TMP_ROOT}/accepted-sequences.txt"
  for seq in $(seq 0 $((src_count - 1))); do
    expect_eq "$(redis_auth "${DATA_PASSWORD}" "${new_master}" get "sentinel:ha:seq:${seq}")" "${seq}" "source business sequence ${seq}"
    expect_eq "$(redis_auth "${DATA_PASSWORD}" "${dst_master}" get "sentinel:ha:seq:${seq}")" "${seq}" "target business sequence ${seq}"
  done
  cp_count=$(redis_auth "${DATA_PASSWORD}" "${dst_master}" hlen redis-gunyu-checkpoint-hash)
  expect_eq "${cp_count}" "1" "checkpoint entries"
  bisync_count=$(redis_auth "${DATA_PASSWORD}" "${dst_master}" --scan --pattern 'redis-gunyu-bisync:*' | wc -l | tr -d '[:space:]')
  expect_eq "${bisync_count}" "0" "bisync metadata"
  stop_pid "${SYNCER_PID_B}"
  SYNCER_PID_B=""
  {
    echo "initial_leader=syncer-a"
    echo "follower_takeover=verified"
    echo "source_master_before=${old_master}"
    echo "source_master_after=${new_master}"
    echo "target_master=${dst_master}"
    echo "acknowledged_writes=${acked}"
    echo "source_sequence_keys=${src_count}"
    echo "target_sequence_keys=${dst_count}"
    echo "checkpoint_entries=${cp_count}"
    echo "bisync_metadata=${bisync_count}"
    echo "replay_mode=${REPLAY_MODE}"
  } >"${TMP_ROOT}/result.txt"
}
run_competition
echo "sentinel_ha_competition_result=${TMP_ROOT}/result.txt"
