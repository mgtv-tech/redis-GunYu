#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_ROOT="${TMPDIR:-/tmp}/redisgunyu-bisync-cat10"
source "${ROOT}/tests/bisync/lib/redis_env.sh"
require_test_commands go docker redis-cli

MODULE_IMAGE="${MODULE_IMAGE:-redis/redis-stack-server:7.4.0-v8@sha256:798ab84d9f266936b034ab11c4d04a2b8e4b441884c5aa7d17ac951eefdf742a}"
MODULE_VERIFY_PORT="${MODULE_VERIFY_PORT:-6389}"
MODULE_SRC_PORT="${MODULE_SRC_PORT:-6390}"
MODULE_DST_PORT="${MODULE_DST_PORT:-6391}"
MODULE_RUN_ID="${TEST_RUN_ID:-$$}"
MODULE_VERIFY_CONTAINER="${MODULE_VERIFY_CONTAINER:-redis-stack-gunyu-test-${MODULE_RUN_ID}}"
MODULE_SRC_CONTAINER="${MODULE_SRC_CONTAINER:-redis-stack-gunyu-src-${MODULE_RUN_ID}}"
MODULE_DST_CONTAINER="${MODULE_DST_CONTAINER:-redis-stack-gunyu-dst-${MODULE_RUN_ID}}"
MODULE_GUNYU_BIN="${TMP_ROOT}/redisGunYu"
MODULE_RDB_FILE="${TMP_ROOT}/redis-stack-gunyu-module-keyspace.rdb"
MODULE_FAIL_CONF="${TMP_ROOT}/redisgunyu_module_rdb_load_fail.yaml"
MODULE_SKIP_CONF="${TMP_ROOT}/redisgunyu_module_rdb_load_skip.yaml"

cleanup() {
  local code=$?
  set +e
  docker rm -f "${MODULE_VERIFY_CONTAINER}" "${MODULE_SRC_CONTAINER}" "${MODULE_DST_CONTAINER}" >/dev/null 2>&1 || true
  if [[ "${KEEP_TMP:-0}" != "1" ]]; then
    rm -rf "${TMP_ROOT}"
  fi
  exit "${code}"
}
trap cleanup EXIT

rm -rf "${TMP_ROOT}"
mkdir -p "${TMP_ROOT}/gocache"

docker_recreate() {
  local name=$1
  local port=$2
  docker rm -f "${name}" >/dev/null 2>&1 || true
  docker run -d --name "${name}" -p "${port}:6379" "${MODULE_IMAGE}" >/dev/null
}

wait_for_ping() {
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

assert_eq() {
  local got=$1
  local want=$2
  local label=$3
  if [[ "${got}" != "${want}" ]]; then
    echo "assertion failed for ${label}: got=[${got}] want=[${want}]" >&2
    exit 1
  fi
}

assert_contains() {
  local haystack=$1
  local needle=$2
  local label=$3
  if [[ "${haystack}" != *"${needle}"* ]]; then
    echo "assertion failed for ${label}: missing [${needle}] in [${haystack}]" >&2
    exit 1
  fi
}

write_rdb_load_conf() {
  local path=$1
  local policy=$2
  cat >"${path}" <<EOF
action: load
rdbPath: "${MODULE_RDB_FILE}"
load:
  redis:
    addresses: [127.0.0.1:${MODULE_DST_PORT}]
    type: standalone
  replay:
    keyExists: replace
    replayRdbEnableRestore: true
    moduleAuxPolicy: ${policy}
    maxProtoBulkLen: 536870912
EOF
}

echo "[1/8] building binaries"
(cd "${ROOT}" && GOCACHE="${TMP_ROOT}/gocache" go build -o "${MODULE_GUNYU_BIN}" ./main.go)

echo "[2/8] starting Redis Stack containers"
docker_recreate "${MODULE_VERIFY_CONTAINER}" "${MODULE_VERIFY_PORT}"
docker_recreate "${MODULE_SRC_CONTAINER}" "${MODULE_SRC_PORT}"
docker_recreate "${MODULE_DST_CONTAINER}" "${MODULE_DST_PORT}"
wait_for_ping "${MODULE_VERIFY_PORT}"
wait_for_ping "${MODULE_SRC_PORT}"
wait_for_ping "${MODULE_DST_PORT}"

echo "[3/8] verifying module keyspec against Redis Stack"
(cd "${ROOT}" && GOCACHE="${TMP_ROOT}/gocache" go run ./tests/bisync/cmd/keyspec_verify --addrs "127.0.0.1:${MODULE_VERIFY_PORT}" --tags module --fail-on-unsupported) | tee "${TMP_ROOT}/keyspec_verify.log"

echo "[4/8] seeding RedisJSON / RedisBloom / RediSearch source data"
docker exec "${MODULE_SRC_CONTAINER}" redis-cli FLUSHALL >/dev/null
docker exec "${MODULE_DST_CONTAINER}" redis-cli FLUSHALL >/dev/null
docker exec "${MODULE_SRC_CONTAINER}" redis-cli JSON.SET doc:2 '$' '{"name":"bob","age":20}' >/dev/null
docker exec "${MODULE_SRC_CONTAINER}" redis-cli BF.ADD bf:2 item-b >/dev/null
docker exec "${MODULE_SRC_CONTAINER}" redis-cli FT.CREATE idx ON JSON PREFIX 1 doc: SCHEMA '$.name' AS name TEXT >/dev/null

assert_eq "$(docker exec "${MODULE_SRC_CONTAINER}" redis-cli JSON.GET doc:2 '$')" '[{"name":"bob","age":20}]' "source JSON"
assert_eq "$(docker exec "${MODULE_SRC_CONTAINER}" redis-cli BF.EXISTS bf:2 item-b)" '1' "source Bloom"
assert_eq "$(docker exec "${MODULE_SRC_CONTAINER}" redis-cli FT._LIST)" 'idx' "source RediSearch index"

echo "[5/8] generating and inspecting source RDB"
docker exec "${MODULE_SRC_CONTAINER}" redis-cli SAVE >/dev/null
docker cp "${MODULE_SRC_CONTAINER}:/data/dump.rdb" "${MODULE_RDB_FILE}"
(cd "${ROOT}" && GOCACHE="${TMP_ROOT}/gocache" "${MODULE_GUNYU_BIN}" -cmd=rdb -rdb.action=print -rdb.rdbPath="${MODULE_RDB_FILE}" -rdb.print.noLogValue=true -rdb.print.moduleAuxPolicy=skip) | tee "${TMP_ROOT}/rdb_print.log"
assert_contains "$(cat "${TMP_ROOT}/rdb_print.log")" '"key":"doc:2"' "RDB print doc:2"
assert_contains "$(cat "${TMP_ROOT}/rdb_print.log")" '"key":"bf:2"' "RDB print bf:2"

echo "[6/8] validating moduleAuxPolicy=fail boundary"
write_rdb_load_conf "${MODULE_FAIL_CONF}" fail
set +e
"${MODULE_GUNYU_BIN}" -cmd=rdb -conf="${MODULE_FAIL_CONF}" >"${TMP_ROOT}/rdb_load_fail.log" 2>&1
fail_status=$?
set -e
if [[ "${fail_status}" == "0" ]]; then
  echo "expected moduleAuxPolicy=fail replay to stop on module aux" >&2
  exit 1
fi
assert_contains "$(cat "${TMP_ROOT}/rdb_load_fail.log")" 'module aux data is unsupported' "moduleAuxPolicy=fail"

echo "[7/8] validating moduleAuxPolicy=skip restore path"
docker exec "${MODULE_DST_CONTAINER}" redis-cli FLUSHALL >/dev/null
write_rdb_load_conf "${MODULE_SKIP_CONF}" skip
"${MODULE_GUNYU_BIN}" -cmd=rdb -conf="${MODULE_SKIP_CONF}" | tee "${TMP_ROOT}/rdb_load_skip.log"

assert_eq "$(docker exec "${MODULE_DST_CONTAINER}" redis-cli DBSIZE)" '2' "destination dbsize"
assert_eq "$(docker exec "${MODULE_DST_CONTAINER}" redis-cli TYPE doc:2)" 'ReJSON-RL' "destination JSON type"
assert_eq "$(docker exec "${MODULE_DST_CONTAINER}" redis-cli TYPE bf:2)" 'MBbloom--' "destination Bloom type"
assert_eq "$(docker exec "${MODULE_DST_CONTAINER}" redis-cli JSON.GET doc:2 '$')" '[{"name":"bob","age":20}]' "destination JSON value"
assert_eq "$(docker exec "${MODULE_DST_CONTAINER}" redis-cli BF.EXISTS bf:2 item-b)" '1' "destination Bloom value"
assert_eq "$(docker exec "${MODULE_DST_CONTAINER}" redis-cli FT._LIST)" '' "destination RediSearch index"

echo "[8/8] category10 summary"
echo "keyspec_log=${TMP_ROOT}/keyspec_verify.log"
echo "rdb_print_log=${TMP_ROOT}/rdb_print.log"
echo "rdb_fail_log=${TMP_ROOT}/rdb_load_fail.log"
echo "rdb_skip_log=${TMP_ROOT}/rdb_load_skip.log"
