#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_ROOT="${TMPDIR:-/tmp}/redisgunyu-nonbisync-cat1"
source "${ROOT}/tests/nonbisync/lib/test_env.sh"
require_test_commands go redis-server redis-cli curl

SCENARIOS="${SCENARIOS:-sync,pipeline}"
SYNC_SRC_BASE="${SYNC_SRC_BASE:-31100}"
SYNC_DST_BASE="${SYNC_DST_BASE:-31200}"
SYNC_HTTP_PORT="${SYNC_HTTP_PORT:-31180}"
PIPE_SRC_BASE="${PIPE_SRC_BASE:-31300}"
PIPE_DST_BASE="${PIPE_DST_BASE:-31400}"
PIPE_HTTP_PORT="${PIPE_HTTP_PORT:-31380}"
TEST_PREFIX="${TEST_PREFIX:-nonbisync:cat1:$(date +%s)}"
SYNCER_PID=""
REDIS_SERVER_BIN="$(resolve_redis_server_bin REDIS_SERVER_BIN REDIS_DEPLOY_ROOT)"

cleanup() {
  local code=$?
  set +e
  stop_pid "${SYNCER_PID:-}"
  shutdown_ports \
    "${SYNC_SRC_BASE}" "$((SYNC_SRC_BASE + 1))" "$((SYNC_SRC_BASE + 2))" \
    "${SYNC_DST_BASE}" "$((SYNC_DST_BASE + 1))" "$((SYNC_DST_BASE + 2))" \
    "${PIPE_SRC_BASE}" "$((PIPE_SRC_BASE + 1))" "$((PIPE_SRC_BASE + 2))" \
    "${PIPE_DST_BASE}" "$((PIPE_DST_BASE + 1))" "$((PIPE_DST_BASE + 2))"
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

assert_expected_state() {
  local port=$1
  local prefix=$2

  expect_eq "$(redis_call cluster "${port}" get "$(key_name "${prefix}" "string")")" "alpha" "$(key_name "${prefix}" "string")"
  expect_eq "$(redis_call cluster "${port}" get "$(key_name "${prefix}" "counter")")" "9" "$(key_name "${prefix}" "counter")"
  expect_eq "$(set_state cluster "${port}" "$(key_name "${prefix}" "set")")" "blue red" "$(key_name "${prefix}" "set")"
  expect_eq "$(hash_state cluster "${port}" "$(key_name "${prefix}" "hash")")" "f1=v1|f2=v2" "$(key_name "${prefix}" "hash")"
  expect_eq "$(list_state cluster "${port}" "$(key_name "${prefix}" "list")")" "a b c" "$(key_name "${prefix}" "list")"
  expect_eq "$(zset_state cluster "${port}" "$(key_name "${prefix}" "zset")")" "one=1|two=2" "$(key_name "${prefix}" "zset")"
  expect_eq "$(redis_call cluster "${port}" get "$(key_name "${prefix}" "txn-counter{txn}")")" "5" "$(key_name "${prefix}" "txn-counter{txn}")"
  expect_eq "$(list_state cluster "${port}" "$(key_name "${prefix}" "txn-list{txn}")")" "x y" "$(key_name "${prefix}" "txn-list{txn}")"
  expect_eq "$(hash_state cluster "${port}" "$(key_name "${prefix}" "txn-hash{txn}")")" "field=value" "$(key_name "${prefix}" "txn-hash{txn}")"
  expect_absent "$(redis_call cluster "${port}" exists "$(key_name "${prefix}" "delete-me")")" "$(key_name "${prefix}" "delete-me")"
}

write_source_data() {
  local port=$1
  local prefix=$2
  redis_cmd cluster "${port}" set "$(key_name "${prefix}" "string")" "alpha"
  redis_cmd cluster "${port}" incrby "$(key_name "${prefix}" "counter")" 9
  redis_cmd cluster "${port}" sadd "$(key_name "${prefix}" "set")" red blue
  redis_cmd cluster "${port}" hset "$(key_name "${prefix}" "hash")" f1 v1 f2 v2
  redis_cmd cluster "${port}" rpush "$(key_name "${prefix}" "list")" a b c
  redis_cmd cluster "${port}" zadd "$(key_name "${prefix}" "zset")" 1 one 2 two
  redis_cmd cluster "${port}" set "$(key_name "${prefix}" "delete-me")" "gone"
  redis_cmd cluster "${port}" del "$(key_name "${prefix}" "delete-me")"
  redis-cli -c -p "${port}" <<EOF >/dev/null
MULTI
INCRBY $(key_name "${prefix}" "txn-counter{txn}") 5
RPUSH $(key_name "${prefix}" "txn-list{txn}") x y
HSET $(key_name "${prefix}" "txn-hash{txn}") field value
EXEC
EOF
}

run_scenario() {
  local name=$1
  local mode=$2
  local src_base dst_base http_port
  local src_ports dst_ports src_csv dst_csv prefix source_master target_master conf_file

  if [[ "${mode}" == "sync" ]]; then
    src_base=${SYNC_SRC_BASE}
    dst_base=${SYNC_DST_BASE}
    http_port=${SYNC_HTTP_PORT}
  else
    src_base=${PIPE_SRC_BASE}
    dst_base=${PIPE_DST_BASE}
    http_port=${PIPE_HTTP_PORT}
  fi

  src_ports=("${src_base}" "$((src_base + 1))" "$((src_base + 2))")
  dst_ports=("${dst_base}" "$((dst_base + 1))" "$((dst_base + 2))")
  src_csv=$(format_addrs "${src_ports[@]}")
  dst_csv=$(format_addrs "${dst_ports[@]}")
  prefix="${TEST_PREFIX}:${name}"

  echo "[category1] scenario=${name} mode=${mode}"
  start_cluster "${REDIS_SERVER_BIN}" "${TMP_ROOT}" "${name}-src" "${src_ports[@]}"
  start_cluster "${REDIS_SERVER_BIN}" "${TMP_ROOT}" "${name}-dst" "${dst_ports[@]}"

  conf_file="${TMP_ROOT}/${name}.yaml"
  write_syncer_conf "${conf_file}" "${http_port}" "${src_csv}" cluster "${dst_csv}" cluster "${TMP_ROOT}/${name}-store" "${mode}"
  SYNCER_PID=$(start_syncer_process "${TMP_ROOT}" "${conf_file}" "${TMP_ROOT}/${name}.log")
  wait_for_syncer "${http_port}"
  sleep 2

  source_master=$(find_first_master_port "${src_ports[@]}")
  target_master=$(find_first_master_port "${dst_ports[@]}")
  write_source_data "${source_master}" "${prefix}"
  write_bulk_dataset cluster "${source_master}" "${prefix}" "steady"
  redis_cmd cluster "${target_master}" set "nonbisync:cat1:isolated:${name}" "target-only"

  wait_for_cluster_equal "${TMP_ROOT}" "${src_csv}" "${dst_csv}" "${prefix}:*"
  cluster_compare "${TMP_ROOT}" "${src_csv}" "${dst_csv}" "${prefix}:*"

  target_master=$(find_first_master_port "${dst_ports[@]}")
  source_master=$(find_first_master_port "${src_ports[@]}")
  assert_expected_state "${target_master}" "${prefix}"
  assert_min_key_count_cluster "${prefix}:*" "$(bulk_dataset_min_keys)" "${dst_ports[@]}"
  expect_absent "$(redis_call cluster "${source_master}" exists "nonbisync:cat1:isolated:${name}")" "isolated target key on source"
  assert_no_bisync_metadata_cluster "${dst_ports[@]}"
  assert_checkpoint_signals_cluster "${target_master}" "${dst_ports[@]}"
  assert_log_has_no_bisync_markers "${TMP_ROOT}/${name}.log"

  stop_pid "${SYNCER_PID}"
  SYNCER_PID=""
  shutdown_ports "${src_ports[@]}" "${dst_ports[@]}"

  echo "prefix=${prefix}"
  echo "log=${TMP_ROOT}/${name}.log"
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
