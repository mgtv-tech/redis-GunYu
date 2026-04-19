#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_ROOT="${TMPDIR:-/tmp}/redisgunyu-nonbisync-cat3"
source "${ROOT}/tests/nonbisync/lib/test_env.sh"

SCENARIOS="${SCENARIOS:-sync,pipeline}"
SYNC_SRC_PORT="${SYNC_SRC_PORT:-31900}"
SYNC_DST_PORT="${SYNC_DST_PORT:-32000}"
SYNC_HTTP_PORT="${SYNC_HTTP_PORT:-31980}"
PIPE_SRC_PORT="${PIPE_SRC_PORT:-32100}"
PIPE_DST_PORT="${PIPE_DST_PORT:-32200}"
PIPE_HTTP_PORT="${PIPE_HTTP_PORT:-32180}"
TEST_PREFIX="${TEST_PREFIX:-nonbisync:cat3:$(date +%s)}"
SYNCER_PID=""
REDIS_SERVER_BIN="$(resolve_redis_server_bin REDIS_SERVER_BIN REDIS_DEPLOY_ROOT)"

cleanup() {
  local code=$?
  set +e
  stop_pid "${SYNCER_PID:-}"
  shutdown_ports "${SYNC_SRC_PORT}" "${SYNC_DST_PORT}" "${PIPE_SRC_PORT}" "${PIPE_DST_PORT}"
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
      redis_cmd standalone "${port}" set "$(key_name "${prefix}" "string")" "s1"
      redis_cmd standalone "${port}" incrby "$(key_name "${prefix}" "counter")" 3
      redis_cmd standalone "${port}" sadd "$(key_name "${prefix}" "set")" red blue
      redis_cmd standalone "${port}" hset "$(key_name "${prefix}" "hash")" f1 v1
      redis_cmd standalone "${port}" rpush "$(key_name "${prefix}" "list")" a b
      redis_cmd standalone "${port}" zadd "$(key_name "${prefix}" "zset")" 1 one
      ;;
    2)
      redis_cmd standalone "${port}" set "$(key_name "${prefix}" "string")" "s2"
      redis_cmd standalone "${port}" incrby "$(key_name "${prefix}" "counter")" 4
      redis_cmd standalone "${port}" sadd "$(key_name "${prefix}" "set")" green
      redis_cmd standalone "${port}" hset "$(key_name "${prefix}" "hash")" f2 v2
      redis_cmd standalone "${port}" rpush "$(key_name "${prefix}" "list")" c d
      redis_cmd standalone "${port}" zadd "$(key_name "${prefix}" "zset")" 2 two
      redis_cmd standalone "${port}" set "$(key_name "${prefix}" "delete-me")" "gone"
      redis_cmd standalone "${port}" del "$(key_name "${prefix}" "delete-me")"
      ;;
  esac
}

assert_expected_state() {
  local port=$1
  local prefix=$2

  expect_eq "$(redis_call standalone "${port}" get "$(key_name "${prefix}" "string")")" "s2" "$(key_name "${prefix}" "string")"
  expect_eq "$(redis_call standalone "${port}" get "$(key_name "${prefix}" "counter")")" "7" "$(key_name "${prefix}" "counter")"
  expect_eq "$(set_state standalone "${port}" "$(key_name "${prefix}" "set")")" "blue green red" "$(key_name "${prefix}" "set")"
  expect_eq "$(hash_state standalone "${port}" "$(key_name "${prefix}" "hash")")" "f1=v1|f2=v2" "$(key_name "${prefix}" "hash")"
  expect_eq "$(list_state standalone "${port}" "$(key_name "${prefix}" "list")")" "a b c d" "$(key_name "${prefix}" "list")"
  expect_eq "$(zset_state standalone "${port}" "$(key_name "${prefix}" "zset")")" "one=1|two=2" "$(key_name "${prefix}" "zset")"
  expect_absent "$(redis_call standalone "${port}" exists "$(key_name "${prefix}" "delete-me")")" "$(key_name "${prefix}" "delete-me")"
}

run_scenario() {
  local name=$1
  local mode=$2
  local src_port dst_port http_port prefix conf_file

  if [[ "${mode}" == "sync" ]]; then
    src_port=${SYNC_SRC_PORT}
    dst_port=${SYNC_DST_PORT}
    http_port=${SYNC_HTTP_PORT}
  else
    src_port=${PIPE_SRC_PORT}
    dst_port=${PIPE_DST_PORT}
    http_port=${PIPE_HTTP_PORT}
  fi

  prefix="${TEST_PREFIX}:${name}"
  echo "[category3] scenario=${name} mode=${mode}"

  start_standalone "${REDIS_SERVER_BIN}" "${TMP_ROOT}" "${name}-src" "${src_port}"
  start_standalone "${REDIS_SERVER_BIN}" "${TMP_ROOT}" "${name}-dst" "${dst_port}"

  conf_file="${TMP_ROOT}/${name}.yaml"
  write_syncer_conf "${conf_file}" "${http_port}" "127.0.0.1:${src_port}" standalone "127.0.0.1:${dst_port}" standalone "${TMP_ROOT}/${name}-store" "${mode}"
  SYNCER_PID=$(start_syncer_process "${TMP_ROOT}" "${conf_file}" "${TMP_ROOT}/${name}.log")
  wait_for_syncer "${http_port}"
  sleep 2

  write_phase "${src_port}" "${prefix}" 1
  wait_for_standalone_equal "${src_port}" "${dst_port}" "${prefix}:*"

  stop_pid "${SYNCER_PID}"
  SYNCER_PID=""

  write_phase "${src_port}" "${prefix}" 2
  redis_cmd standalone "${dst_port}" set "nonbisync:cat3:isolated:${name}" "target-only"

  SYNCER_PID=$(start_syncer_process "${TMP_ROOT}" "${conf_file}" "${TMP_ROOT}/${name}.restart.log")
  wait_for_syncer "${http_port}"
  sleep 2

  wait_for_standalone_equal "${src_port}" "${dst_port}" "${prefix}:*"

  assert_expected_state "${dst_port}" "${prefix}"
  expect_absent "$(redis_call standalone "${src_port}" exists "nonbisync:cat3:isolated:${name}")" "isolated target key on source"
  assert_no_bisync_metadata_standalone "${dst_port}"
  assert_checkpoint_signals_standalone "${dst_port}"
  assert_log_has_no_bisync_markers "${TMP_ROOT}/${name}.log"
  assert_log_has_no_bisync_markers "${TMP_ROOT}/${name}.restart.log"

  stop_pid "${SYNCER_PID}"
  SYNCER_PID=""
  shutdown_ports "${src_port}" "${dst_port}"

  echo "prefix=${prefix}"
  echo "restart_log=${TMP_ROOT}/${name}.restart.log"
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
