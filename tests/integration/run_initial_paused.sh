#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_ROOT="${TMPDIR:-/tmp}/redisgunyu-initial-paused"
source "${ROOT}/tests/nonbisync/lib/test_env.sh"
require_test_commands go redis-server redis-cli curl

SRC_PORT="${INITIAL_PAUSED_SRC_PORT:-31940}"
DST_PORT="${INITIAL_PAUSED_DST_PORT:-31941}"
HTTP_PORT="${INITIAL_PAUSED_HTTP_PORT:-31942}"
TEST_PREFIX="${TEST_PREFIX:-initial-paused:$(date +%s)}"
SYNCER_PID=""
SRC_REDIS_PID=""
DST_REDIS_PID=""
TMP_ROOT_OWNED=0
REDIS_SERVER_BIN="$(resolve_redis_server_bin REDIS_SERVER_BIN REDIS_DEPLOY_ROOT)"

port_is_open() {
  local port=$1
  (echo >/dev/tcp/127.0.0.1/"${port}") >/dev/null 2>&1
}

validate_ports() {
  local name port
  for name in SRC_PORT DST_PORT HTTP_PORT; do
    port=${!name}
    if [[ ! "${port}" =~ ^[1-9][0-9]*$ ]] || (( port > 65535 )); then
      echo "${name} must be an integer between 1 and 65535: ${port}" >&2
      return 1
    fi
  done
  if [[ "${SRC_PORT}" == "${DST_PORT}" || "${SRC_PORT}" == "${HTTP_PORT}" || "${DST_PORT}" == "${HTTP_PORT}" ]]; then
    echo "initial-paused source, target, and HTTP ports must be distinct" >&2
    return 1
  fi
}

refuse_occupied_ports() {
  local port
  for port in "${SRC_PORT}" "${DST_PORT}" "${HTTP_PORT}"; do
    if port_is_open "${port}"; then
      echo "refusing to run initial-paused test: port ${port} is already occupied" >&2
      return 1
    fi
  done
}

redis_process_matches() {
  local pid=$1
  local instance_dir=$2
  local port=$3
  local command
  command=$(ps -p "${pid}" -o command= 2>/dev/null || true)
  [[ "${command}" == *redis-server* ]] && \
    { [[ "${command}" =~ :${port}([[:space:]]|$) ]] || [[ "${command}" == *"${instance_dir}/redis.conf"* ]]; }
}

capture_owned_redis_pid() {
  local instance_dir=$1
  local port=$2
  local output_variable=$3
  local pid_file="${instance_dir}/redis.pid"
  local pid=""
  local attempt
  for attempt in $(seq 1 25); do
    if [[ -f "${pid_file}" ]]; then
      pid=$(tr -d '[:space:]' < "${pid_file}")
      if [[ "${pid}" =~ ^[0-9]+$ ]] && kill -0 "${pid}" >/dev/null 2>&1; then
        printf -v "${output_variable}" '%s' "${pid}"
        if redis_process_matches "${pid}" "${instance_dir}" "${port}"; then
          return 0
        fi
      fi
    fi
    sleep 0.1
  done
  echo "failed to identify Redis started for port ${port} from ${pid_file}" >&2
  return 1
}

start_owned_standalone() {
  local prefix=$1
  local port=$2
  local output_variable=$3
  local instance_dir="${TMP_ROOT}/${prefix}-${port}"

  write_standalone_conf "${instance_dir}" "${port}"
  "${REDIS_SERVER_BIN}" "${instance_dir}/redis.conf"
  capture_owned_redis_pid "${instance_dir}" "${port}" "${output_variable}"
  wait_for_ping "${port}"
}

stop_owned_redis() {
  local expected_pid=$1
  local instance_dir=$2
  local port=$3
  local pid_file="${instance_dir}/redis.pid"
  local current_pid=""
  local attempt

  [[ "${expected_pid}" =~ ^[0-9]+$ ]] || return 0
  [[ -f "${pid_file}" ]] || return 0
  current_pid=$(tr -d '[:space:]' < "${pid_file}")
  if [[ "${current_pid}" != "${expected_pid}" ]]; then
    echo "refusing to stop Redis on port ${port}: pidfile no longer identifies the process started by this run" >&2
    return 0
  fi
  if ! kill -0 "${expected_pid}" >/dev/null 2>&1; then
    return 0
  fi
  if ! redis_process_matches "${expected_pid}" "${instance_dir}" "${port}"; then
    echo "refusing to stop pid ${expected_pid}: it is not the Redis instance created for port ${port}" >&2
    return 0
  fi

  kill "${expected_pid}" >/dev/null 2>&1 || true
  for attempt in $(seq 1 40); do
    if ! kill -0 "${expected_pid}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  if redis_process_matches "${expected_pid}" "${instance_dir}" "${port}"; then
    kill -KILL "${expected_pid}" >/dev/null 2>&1 || true
  fi
}

cleanup() {
  local code=$?
  set +e
  stop_pid "${SYNCER_PID:-}"
  stop_owned_redis "${SRC_REDIS_PID:-}" "${TMP_ROOT}/source-${SRC_PORT}" "${SRC_PORT}"
  stop_owned_redis "${DST_REDIS_PID:-}" "${TMP_ROOT}/target-${DST_PORT}" "${DST_PORT}"
  if [[ "${KEEP_TMP:-0}" != "1" && "${TMP_ROOT_OWNED}" == "1" ]]; then
    rm -rf -- "${TMP_ROOT}"
  fi
  exit "${code}"
}
trap cleanup EXIT

wait_for_syncer_state() {
  local expected=$1
  local status
  for _ in $(seq 1 100); do
    status=$(curl -sf "http://127.0.0.1:${HTTP_PORT}/syncer/status" 2>/dev/null || true)
    if [[ "${status}" == *'"Role":"leader"'* && "${status}" == *"\"State\":\"${expected}\""* ]]; then
      return 0
    fi
    sleep 0.2
  done
  echo "syncer did not reach state ${expected}" >&2
  curl -s "http://127.0.0.1:${HTTP_PORT}/syncer/status" >&2 || true
  return 1
}

wait_for_value() {
  local key=$1
  local expected=$2
  local actual
  for _ in $(seq 1 100); do
    actual=$(redis_call standalone "${DST_PORT}" get "${key}")
    if [[ "${actual}" == "${expected}" ]]; then
      return 0
    fi
    sleep 0.2
  done
  echo "target key ${key} did not become ${expected}" >&2
  return 1
}

syncer_start_count() {
  grep -c "start syncer" "${LOG_FILE}" 2>/dev/null || true
}

wait_for_new_syncer() {
  local previous_count=$1
  local current_count
  for _ in $(seq 1 100); do
    current_count=$(syncer_start_count)
    if (( current_count > previous_count )); then
      return 0
    fi
    sleep 0.2
  done
  echo "syncer was not recreated; start count remained ${previous_count}" >&2
  return 1
}

validate_ports
refuse_occupied_ports

rm -rf -- "${TMP_ROOT}"
mkdir -p "${TMP_ROOT}"
TMP_ROOT_OWNED=1

build_nonbisync_binaries "${TMP_ROOT}"
start_owned_standalone source "${SRC_PORT}" SRC_REDIS_PID
start_owned_standalone target "${DST_PORT}" DST_REDIS_PID

CONF_FILE="${TMP_ROOT}/gunyu.yaml"
LOG_FILE="${TMP_ROOT}/gunyu.log"
write_syncer_conf "${CONF_FILE}" "${HTTP_PORT}" "127.0.0.1:${SRC_PORT}" standalone \
  "127.0.0.1:${DST_PORT}" standalone "${TMP_ROOT}/store" sync true

SYNCER_PID=$(start_syncer_process "${TMP_ROOT}" "${CONF_FILE}" "${LOG_FILE}")
wait_for_syncer "${HTTP_PORT}"
wait_for_syncer_state pause

BEFORE_KEY="${TEST_PREFIX}:before-resume"
redis_cmd standalone "${SRC_PORT}" set "${BEFORE_KEY}" before
sleep 1
expect_absent "$(redis_call standalone "${DST_PORT}" exists "${BEFORE_KEY}")" "data written before initial resume"
expect_eq "$(redis_call standalone "${DST_PORT}" dbsize)" "0" "target DB before initial resume"
expect_eq "$(redis-cli -p "${SRC_PORT}" info replication | awk -F: '$1=="connected_slaves" {gsub("\r","",$2); print $2; exit}')" "0" "source replication clients before initial resume"

curl -sf -XPOST "http://127.0.0.1:${HTTP_PORT}/syncer/resume?inputs=all" >/dev/null
curl -sf -XPOST "http://127.0.0.1:${HTTP_PORT}/syncer/resume?inputs=all" >/dev/null
wait_for_syncer_state run
wait_for_value "${BEFORE_KEY}" before

curl -sf -XPOST "http://127.0.0.1:${HTTP_PORT}/syncer/pause?inputs=all" >/dev/null
curl -sf -XPOST "http://127.0.0.1:${HTTP_PORT}/syncer/pause?inputs=all" >/dev/null
wait_for_syncer_state pause
DURING_KEY="${TEST_PREFIX}:during-pause"
redis_cmd standalone "${SRC_PORT}" set "${DURING_KEY}" during
sleep 1
expect_absent "$(redis_call standalone "${DST_PORT}" exists "${DURING_KEY}")" "data written during runtime pause"

START_COUNT=$(syncer_start_count)
curl -sf -XPOST "http://127.0.0.1:${HTTP_PORT}/syncer/restart" >/dev/null
wait_for_new_syncer "${START_COUNT}"
wait_for_syncer_state pause
expect_eq "$(redis-cli -p "${SRC_PORT}" info replication | awk -F: '$1=="connected_slaves" {gsub("\r","",$2); print $2; exit}')" "0" "source replication clients after paused restart"
expect_absent "$(redis_call standalone "${DST_PORT}" exists "${DURING_KEY}")" "data written before paused restart"

curl -sf -XPOST "http://127.0.0.1:${HTTP_PORT}/syncer/resume?inputs=all" >/dev/null
wait_for_syncer_state run
wait_for_value "${DURING_KEY}" during

START_COUNT=$(syncer_start_count)
curl -sf -XPOST "http://127.0.0.1:${HTTP_PORT}/syncer/restart" >/dev/null
wait_for_new_syncer "${START_COUNT}"
wait_for_syncer_state run
AFTER_RESTART_KEY="${TEST_PREFIX}:after-restart"
redis_cmd standalone "${SRC_PORT}" set "${AFTER_RESTART_KEY}" restarted
wait_for_value "${AFTER_RESTART_KEY}" restarted

stop_pid "${SYNCER_PID}"
SYNCER_PID=""
SYNCER_PID=$(start_syncer_process "${TMP_ROOT}" "${CONF_FILE}" "${TMP_ROOT}/gunyu.initial-stop.log")
wait_for_syncer_state pause
curl -sf -XDELETE "http://127.0.0.1:${HTTP_PORT}/" >/dev/null
wait_for_pid_exit "${SYNCER_PID}" 10
SYNCER_PID=""

echo "initialPaused standalone lifecycle passed"
