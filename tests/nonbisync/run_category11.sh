#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_ROOT="${TMPDIR:-/tmp}/redisgunyu-nonbisync-cat11"
source "${ROOT}/tests/nonbisync/lib/test_env.sh"
require_test_commands go docker redis-cli curl

MODULE_IMAGE="${MODULE_IMAGE:-redis/redis-stack-server:7.4.0-v8@sha256:798ab84d9f266936b034ab11c4d04a2b8e4b441884c5aa7d17ac951eefdf742a}"
SRC_PORT="${SRC_PORT:-32300}"
DST_PORT="${DST_PORT:-32400}"
HTTP_PORT="${HTTP_PORT:-32380}"
MODULE_RUN_ID="${TEST_RUN_ID:-$$}"
SRC_CONTAINER="${SRC_CONTAINER:-redis-stack-gunyu-incr-src-${MODULE_RUN_ID}}"
DST_CONTAINER="${DST_CONTAINER:-redis-stack-gunyu-incr-dst-${MODULE_RUN_ID}}"
TEST_PREFIX="${TEST_PREFIX:-nonbisync:cat11:$(date +%s)}"
SYNCER_PID=""

cleanup() {
  local code=$?
  set +e
  stop_pid "${SYNCER_PID:-}"
  docker rm -f "${SRC_CONTAINER}" "${DST_CONTAINER}" >/dev/null 2>&1 || true
  if [[ "${KEEP_TMP:-0}" != "1" ]]; then
    rm -rf "${TMP_ROOT}"
  fi
  exit "${code}"
}
trap cleanup EXIT

rm -rf "${TMP_ROOT}"
mkdir -p "${TMP_ROOT}"

docker_recreate() {
  local name=$1
  local port=$2
  docker rm -f "${name}" >/dev/null 2>&1 || true
  docker run -d --name "${name}" -p "${port}:6379" "${MODULE_IMAGE}" >/dev/null
}

wait_for_stack() {
  local port=$1
  for _ in $(seq 1 60); do
    if redis-cli -p "${port}" ping >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  echo "redis stack on port ${port} did not become ready" >&2
  return 1
}

wait_for_json_value() {
  local port=$1
  local key=$2
  local want=$3
  for _ in $(seq 1 80); do
    if [[ "$(redis_call standalone "${port}" JSON.GET "${key}" '$' 2>/dev/null || true)" == "${want}" ]]; then
      return 0
    fi
    sleep 0.5
  done
  echo "json value did not converge for ${key}" >&2
  return 1
}

wait_for_bf_exists() {
  local port=$1
  local key=$2
  local item=$3
  local want=$4
  for _ in $(seq 1 80); do
    if [[ "$(redis_call standalone "${port}" BF.EXISTS "${key}" "${item}" 2>/dev/null || true)" == "${want}" ]]; then
      return 0
    fi
    sleep 0.5
  done
  echo "bloom membership did not converge for ${key}/${item}" >&2
  return 1
}

wait_for_ft_list_contains() {
  local port=$1
  local index=$2
  for _ in $(seq 1 80); do
    if [[ "$(redis_call standalone "${port}" FT._LIST 2>/dev/null || true)" == *"${index}"* ]]; then
      return 0
    fi
    sleep 0.5
  done
  echo "ft index ${index} did not appear" >&2
  return 1
}

wait_for_ft_list_absent() {
  local port=$1
  local index=$2
  for _ in $(seq 1 80); do
    if [[ "$(redis_call standalone "${port}" FT._LIST 2>/dev/null || true)" != *"${index}"* ]]; then
      return 0
    fi
    sleep 0.5
  done
  echo "ft index ${index} did not disappear" >&2
  return 1
}

write_module_syncer_conf() {
  local file=$1
  local http_port=$2
  local storer_dir=$3

  mkdir -p "${storer_dir}"
  cat > "${file}" <<EOF
server:
  listen: 127.0.0.1:${http_port}
  listenPeer: 127.0.0.1:${http_port}
  gracefullStopTimeout: 1s
  checkRedisTypologyTicker: 2s
input:
  redis:
    addresses: [127.0.0.1:${SRC_PORT}]
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
    addresses: [127.0.0.1:${DST_PORT}]
    type: standalone
    version: "7.0.11"
  replay:
    resumeFromBreakPoint: true
    keyExists: replace
    metric: false
    targetDb: -1
    replayTransaction: true
    bisyncEnabled: false
    mode: sync
    moduleAuxPolicy: skip
log:
  level: info
  handler:
    stdout: true
  withCaller: false
  withFunc: false
EOF
}

echo "[1/6] building binaries"
build_nonbisync_binaries "${TMP_ROOT}"

echo "[2/6] starting Redis Stack source and destination"
docker_recreate "${SRC_CONTAINER}" "${SRC_PORT}"
docker_recreate "${DST_CONTAINER}" "${DST_PORT}"
wait_for_stack "${SRC_PORT}"
wait_for_stack "${DST_PORT}"

echo "[3/6] starting standalone non-bisync syncer"
conf_file="${TMP_ROOT}/module-sync.yaml"
write_module_syncer_conf "${conf_file}" "${HTTP_PORT}" "${TMP_ROOT}/store"
SYNCER_PID=$(start_syncer_process "${TMP_ROOT}" "${conf_file}" "${TMP_ROOT}/module-sync.log")
wait_for_syncer "${HTTP_PORT}"
sleep 2

echo "[4/6] replaying RedisJSON and RedisBloom incremental commands"
redis_cmd standalone "${SRC_PORT}" FLUSHALL
redis_cmd standalone "${DST_PORT}" FLUSHALL
redis_cmd standalone "${SRC_PORT}" JSON.SET "${TEST_PREFIX}:doc" '$' '{"name":"alice","age":18}'
redis_cmd standalone "${SRC_PORT}" JSON.DEL "${TEST_PREFIX}:doc" '$.age'
redis_cmd standalone "${SRC_PORT}" JSON.MSET "${TEST_PREFIX}:doc1" '$' '{"a":1}' "${TEST_PREFIX}:doc2" '$' '{"b":2}'
redis_cmd standalone "${SRC_PORT}" BF.ADD "${TEST_PREFIX}:bf" item-a

wait_for_json_value "${DST_PORT}" "${TEST_PREFIX}:doc" '[{"name":"alice"}]'
wait_for_json_value "${DST_PORT}" "${TEST_PREFIX}:doc1" '[{"a":1}]'
wait_for_json_value "${DST_PORT}" "${TEST_PREFIX}:doc2" '[{"b":2}]'
wait_for_bf_exists "${DST_PORT}" "${TEST_PREFIX}:bf" item-a 1

echo "[5/6] replaying RediSearch incremental commands"
redis_cmd standalone "${SRC_PORT}" FT.CREATE "${TEST_PREFIX}:idx" ON JSON PREFIX 1 "${TEST_PREFIX}:doc:" SCHEMA '$.name' AS name TEXT
wait_for_ft_list_contains "${DST_PORT}" "${TEST_PREFIX}:idx"
redis_cmd standalone "${SRC_PORT}" FT.DROPINDEX "${TEST_PREFIX}:idx"
wait_for_ft_list_absent "${DST_PORT}" "${TEST_PREFIX}:idx"

echo "[6/6] category11 summary"
echo "prefix=${TEST_PREFIX}"
echo "sync_log=${TMP_ROOT}/module-sync.log"
