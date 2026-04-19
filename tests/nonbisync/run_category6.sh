#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_ROOT="${TMPDIR:-/tmp}/redisgunyu-nonbisync-cat6"
source "${ROOT}/tests/nonbisync/lib/test_env.sh"

SCENARIOS="${SCENARIOS:-sync,pipeline}"
SYNC_C2S_SRC_BASE="${SYNC_C2S_SRC_BASE:-33100}"
SYNC_C2S_DST_PORT="${SYNC_C2S_DST_PORT:-33200}"
SYNC_C2S_HTTP_PORT="${SYNC_C2S_HTTP_PORT:-33180}"
SYNC_S2C_SRC_PORT="${SYNC_S2C_SRC_PORT:-33300}"
SYNC_S2C_DST_BASE="${SYNC_S2C_DST_BASE:-33400}"
SYNC_S2C_HTTP_PORT="${SYNC_S2C_HTTP_PORT:-33380}"
PIPE_C2S_SRC_BASE="${PIPE_C2S_SRC_BASE:-33500}"
PIPE_C2S_DST_PORT="${PIPE_C2S_DST_PORT:-33600}"
PIPE_C2S_HTTP_PORT="${PIPE_C2S_HTTP_PORT:-33580}"
PIPE_S2C_SRC_PORT="${PIPE_S2C_SRC_PORT:-33700}"
PIPE_S2C_DST_BASE="${PIPE_S2C_DST_BASE:-33800}"
PIPE_S2C_HTTP_PORT="${PIPE_S2C_HTTP_PORT:-33780}"
TEST_PREFIX="${TEST_PREFIX:-nonbisync:cat6:$(date +%s)}"
SYNCER_PID=""
REDIS_SERVER_BIN="$(resolve_redis_server_bin REDIS_SERVER_BIN REDIS_DEPLOY_ROOT)"

cleanup() {
  local code=$?
  set +e
  stop_pid "${SYNCER_PID:-}"
  shutdown_ports \
    "${SYNC_C2S_SRC_BASE}" "$((SYNC_C2S_SRC_BASE + 1))" "$((SYNC_C2S_SRC_BASE + 2))" "$((SYNC_C2S_SRC_BASE + 3))" "$((SYNC_C2S_SRC_BASE + 4))" "$((SYNC_C2S_SRC_BASE + 5))" \
    "${SYNC_C2S_DST_PORT}" \
    "${SYNC_S2C_SRC_PORT}" \
    "${SYNC_S2C_DST_BASE}" "$((SYNC_S2C_DST_BASE + 1))" "$((SYNC_S2C_DST_BASE + 2))" "$((SYNC_S2C_DST_BASE + 3))" "$((SYNC_S2C_DST_BASE + 4))" "$((SYNC_S2C_DST_BASE + 5))" \
    "${PIPE_C2S_SRC_BASE}" "$((PIPE_C2S_SRC_BASE + 1))" "$((PIPE_C2S_SRC_BASE + 2))" "$((PIPE_C2S_SRC_BASE + 3))" "$((PIPE_C2S_SRC_BASE + 4))" "$((PIPE_C2S_SRC_BASE + 5))" \
    "${PIPE_C2S_DST_PORT}" \
    "${PIPE_S2C_SRC_PORT}" \
    "${PIPE_S2C_DST_BASE}" "$((PIPE_S2C_DST_BASE + 1))" "$((PIPE_S2C_DST_BASE + 2))" "$((PIPE_S2C_DST_BASE + 3))" "$((PIPE_S2C_DST_BASE + 4))" "$((PIPE_S2C_DST_BASE + 5))"
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
  local redis_mode=$1
  local port=$2
  local prefix=$3
  local phase=$4
  case "${phase}" in
    1)
      redis_cmd "${redis_mode}" "${port}" set "$(key_name "${prefix}" "string")" "p1"
      redis_cmd "${redis_mode}" "${port}" incrby "$(key_name "${prefix}" "counter")" 2
      redis_cmd "${redis_mode}" "${port}" sadd "$(key_name "${prefix}" "set")" red
      redis_cmd "${redis_mode}" "${port}" hset "$(key_name "${prefix}" "hash")" f1 v1
      redis_cmd "${redis_mode}" "${port}" rpush "$(key_name "${prefix}" "list")" a b
      redis_cmd "${redis_mode}" "${port}" zadd "$(key_name "${prefix}" "zset")" 1 one
      ;;
    2)
      redis_cmd "${redis_mode}" "${port}" set "$(key_name "${prefix}" "string")" "p2"
      redis_cmd "${redis_mode}" "${port}" incrby "$(key_name "${prefix}" "counter")" 3
      redis_cmd "${redis_mode}" "${port}" sadd "$(key_name "${prefix}" "set")" blue
      redis_cmd "${redis_mode}" "${port}" hset "$(key_name "${prefix}" "hash")" f2 v2
      redis_cmd "${redis_mode}" "${port}" rpush "$(key_name "${prefix}" "list")" c
      redis_cmd "${redis_mode}" "${port}" zadd "$(key_name "${prefix}" "zset")" 2 two
      ;;
    3)
      redis_cmd "${redis_mode}" "${port}" set "$(key_name "${prefix}" "delete-me")" "gone"
      redis_cmd "${redis_mode}" "${port}" del "$(key_name "${prefix}" "delete-me")"
      if [[ "${redis_mode}" == "cluster" ]]; then
        redis-cli -c -p "${port}" <<EOF >/dev/null
MULTI
INCRBY $(key_name "${prefix}" "txn-counter{txn}") 4
RPUSH $(key_name "${prefix}" "txn-list{txn}") x y
HSET $(key_name "${prefix}" "txn-hash{txn}") field value
EXEC
EOF
      else
        redis-cli -p "${port}" <<EOF >/dev/null
MULTI
INCRBY $(key_name "${prefix}" "txn-counter{txn}") 4
RPUSH $(key_name "${prefix}" "txn-list{txn}") x y
HSET $(key_name "${prefix}" "txn-hash{txn}") field value
EXEC
EOF
      fi
      ;;
  esac
}

assert_expected_standalone_state() {
  local port=$1
  local prefix=$2
  expect_eq "$(redis_call standalone "${port}" get "$(key_name "${prefix}" "string")")" "p2" "$(key_name "${prefix}" "string")"
  expect_eq "$(redis_call standalone "${port}" get "$(key_name "${prefix}" "counter")")" "5" "$(key_name "${prefix}" "counter")"
  expect_eq "$(set_state standalone "${port}" "$(key_name "${prefix}" "set")")" "blue red" "$(key_name "${prefix}" "set")"
  expect_eq "$(hash_state standalone "${port}" "$(key_name "${prefix}" "hash")")" "f1=v1|f2=v2" "$(key_name "${prefix}" "hash")"
  expect_eq "$(list_state standalone "${port}" "$(key_name "${prefix}" "list")")" "a b c" "$(key_name "${prefix}" "list")"
  expect_eq "$(zset_state standalone "${port}" "$(key_name "${prefix}" "zset")")" "one=1|two=2" "$(key_name "${prefix}" "zset")"
  expect_eq "$(redis_call standalone "${port}" get "$(key_name "${prefix}" "txn-counter{txn}")")" "4" "$(key_name "${prefix}" "txn-counter{txn}")"
  expect_eq "$(list_state standalone "${port}" "$(key_name "${prefix}" "txn-list{txn}")")" "x y" "$(key_name "${prefix}" "txn-list{txn}")"
  expect_eq "$(hash_state standalone "${port}" "$(key_name "${prefix}" "txn-hash{txn}")")" "field=value" "$(key_name "${prefix}" "txn-hash{txn}")"
  expect_absent "$(redis_call standalone "${port}" exists "$(key_name "${prefix}" "delete-me")")" "$(key_name "${prefix}" "delete-me")"
}

assert_expected_cluster_state() {
  local port=$1
  local prefix=$2
  expect_eq "$(redis_call cluster "${port}" get "$(key_name "${prefix}" "string")")" "p2" "$(key_name "${prefix}" "string")"
  expect_eq "$(redis_call cluster "${port}" get "$(key_name "${prefix}" "counter")")" "5" "$(key_name "${prefix}" "counter")"
  expect_eq "$(set_state cluster "${port}" "$(key_name "${prefix}" "set")")" "blue red" "$(key_name "${prefix}" "set")"
  expect_eq "$(hash_state cluster "${port}" "$(key_name "${prefix}" "hash")")" "f1=v1|f2=v2" "$(key_name "${prefix}" "hash")"
  expect_eq "$(list_state cluster "${port}" "$(key_name "${prefix}" "list")")" "a b c" "$(key_name "${prefix}" "list")"
  expect_eq "$(zset_state cluster "${port}" "$(key_name "${prefix}" "zset")")" "one=1|two=2" "$(key_name "${prefix}" "zset")"
  expect_eq "$(redis_call cluster "${port}" get "$(key_name "${prefix}" "txn-counter{txn}")")" "4" "$(key_name "${prefix}" "txn-counter{txn}")"
  expect_eq "$(list_state cluster "${port}" "$(key_name "${prefix}" "txn-list{txn}")")" "x y" "$(key_name "${prefix}" "txn-list{txn}")"
  expect_eq "$(hash_state cluster "${port}" "$(key_name "${prefix}" "txn-hash{txn}")")" "field=value" "$(key_name "${prefix}" "txn-hash{txn}")"
  expect_absent "$(redis_call cluster "${port}" exists "$(key_name "${prefix}" "delete-me")")" "$(key_name "${prefix}" "delete-me")"
}

run_cluster_to_standalone() {
  local name=$1
  local mode=$2
  local src_base dst_port http_port
  local src_ports src_csv prefix source_master source_replica conf_file

  if [[ "${mode}" == "sync" ]]; then
    src_base=${SYNC_C2S_SRC_BASE}
    dst_port=${SYNC_C2S_DST_PORT}
    http_port=${SYNC_C2S_HTTP_PORT}
  else
    src_base=${PIPE_C2S_SRC_BASE}
    dst_port=${PIPE_C2S_DST_PORT}
    http_port=${PIPE_C2S_HTTP_PORT}
  fi

  src_ports=("${src_base}" "$((src_base + 1))" "$((src_base + 2))" "$((src_base + 3))" "$((src_base + 4))" "$((src_base + 5))")
  src_csv=$(format_addrs "${src_ports[@]}")
  prefix="${TEST_PREFIX}:${name}:c2s"

  echo "[category6] scenario=${name} topology=cluster-to-standalone mode=${mode}"
  start_cluster_with_replicas "${REDIS_SERVER_BIN}" "${TMP_ROOT}" "${name}-c2s-src" "${src_ports[@]}"
  start_standalone "${REDIS_SERVER_BIN}" "${TMP_ROOT}" "${name}-c2s-dst" "${dst_port}"

  conf_file="${TMP_ROOT}/${name}-c2s.yaml"
  write_syncer_conf "${conf_file}" "${http_port}" "${src_csv}" cluster "\"127.0.0.1:${dst_port}\"" standalone "${TMP_ROOT}/${name}-c2s-store" "${mode}"
  SYNCER_PID=$(start_syncer_process "${TMP_ROOT}" "${conf_file}" "${TMP_ROOT}/${name}-c2s.log")
  wait_for_syncer "${http_port}"
  sleep 2

  source_master=$(find_first_master_port "${src_ports[@]}")
  write_phase cluster "${source_master}" "${prefix}" 1
  wait_for_redis_equal "${TMP_ROOT}" "${src_csv}" cluster 0 "127.0.0.1:${dst_port}" standalone 0 "${prefix}:*"

  source_replica=$(find_first_replica_port "${src_ports[@]}")
  wait_for_replica_caught_up "${source_replica}"
  force_failover "${source_replica}"
  for port in "${src_ports[@]}"; do
    wait_for_cluster_ok "${port}"
  done
  source_master=$(find_first_master_port "${src_ports[@]}")
  write_phase cluster "${source_master}" "${prefix}" 2
  wait_for_redis_equal "${TMP_ROOT}" "${src_csv}" cluster 0 "127.0.0.1:${dst_port}" standalone 0 "${prefix}:*"

  stop_pid "${SYNCER_PID}"
  SYNCER_PID=""
  write_phase cluster "${source_master}" "${prefix}" 3
  redis_cmd standalone "${dst_port}" set "nonbisync:cat6:c2s:isolated:${name}" "target-only"

  SYNCER_PID=$(start_syncer_process "${TMP_ROOT}" "${conf_file}" "${TMP_ROOT}/${name}-c2s.restart.log")
  wait_for_syncer "${http_port}"
  sleep 2

  wait_for_redis_equal "${TMP_ROOT}" "${src_csv}" cluster 0 "127.0.0.1:${dst_port}" standalone 0 "${prefix}:*"
  source_master=$(find_first_master_port "${src_ports[@]}")
  assert_expected_standalone_state "${dst_port}" "${prefix}"
  expect_absent "$(redis_call cluster "${source_master}" exists "nonbisync:cat6:c2s:isolated:${name}")" "cluster->standalone isolated target key on source"
  assert_no_bisync_metadata_standalone "${dst_port}"
  assert_checkpoint_signals_standalone "${dst_port}"
  assert_log_has_no_bisync_markers "${TMP_ROOT}/${name}-c2s.log"
  assert_log_has_no_bisync_markers "${TMP_ROOT}/${name}-c2s.restart.log"
  assert_log_has_resume_signal "${TMP_ROOT}/${name}-c2s.restart.log"

  stop_pid "${SYNCER_PID}"
  SYNCER_PID=""
  shutdown_ports "${src_ports[@]}" "${dst_port}"
}

run_standalone_to_cluster() {
  local name=$1
  local mode=$2
  local src_port dst_base http_port
  local dst_ports dst_csv prefix target_master target_replica conf_file

  if [[ "${mode}" == "sync" ]]; then
    src_port=${SYNC_S2C_SRC_PORT}
    dst_base=${SYNC_S2C_DST_BASE}
    http_port=${SYNC_S2C_HTTP_PORT}
  else
    src_port=${PIPE_S2C_SRC_PORT}
    dst_base=${PIPE_S2C_DST_BASE}
    http_port=${PIPE_S2C_HTTP_PORT}
  fi

  dst_ports=("${dst_base}" "$((dst_base + 1))" "$((dst_base + 2))" "$((dst_base + 3))" "$((dst_base + 4))" "$((dst_base + 5))")
  dst_csv=$(format_addrs "${dst_ports[@]}")
  prefix="${TEST_PREFIX}:${name}:s2c"

  echo "[category6] scenario=${name} topology=standalone-to-cluster mode=${mode}"
  start_standalone "${REDIS_SERVER_BIN}" "${TMP_ROOT}" "${name}-s2c-src" "${src_port}"
  start_cluster_with_replicas "${REDIS_SERVER_BIN}" "${TMP_ROOT}" "${name}-s2c-dst" "${dst_ports[@]}"

  conf_file="${TMP_ROOT}/${name}-s2c.yaml"
  write_syncer_conf "${conf_file}" "${http_port}" "\"127.0.0.1:${src_port}\"" standalone "${dst_csv}" cluster "${TMP_ROOT}/${name}-s2c-store" "${mode}"
  SYNCER_PID=$(start_syncer_process "${TMP_ROOT}" "${conf_file}" "${TMP_ROOT}/${name}-s2c.log")
  wait_for_syncer "${http_port}"
  sleep 2

  write_phase standalone "${src_port}" "${prefix}" 1
  wait_for_redis_equal "${TMP_ROOT}" "127.0.0.1:${src_port}" standalone 0 "${dst_csv}" cluster 0 "${prefix}:*"

  target_replica=$(find_first_replica_port "${dst_ports[@]}")
  wait_for_replica_caught_up "${target_replica}"
  force_failover "${target_replica}"
  for port in "${dst_ports[@]}"; do
    wait_for_cluster_ok "${port}"
  done
  write_phase standalone "${src_port}" "${prefix}" 2
  wait_for_redis_equal "${TMP_ROOT}" "127.0.0.1:${src_port}" standalone 0 "${dst_csv}" cluster 0 "${prefix}:*"

  stop_pid "${SYNCER_PID}"
  SYNCER_PID=""
  write_phase standalone "${src_port}" "${prefix}" 3
  target_master=$(find_first_master_port "${dst_ports[@]}")
  redis_cmd cluster "${target_master}" set "nonbisync:cat6:s2c:isolated:${name}" "target-only"

  SYNCER_PID=$(start_syncer_process "${TMP_ROOT}" "${conf_file}" "${TMP_ROOT}/${name}-s2c.restart.log")
  wait_for_syncer "${http_port}"
  sleep 2

  wait_for_redis_equal "${TMP_ROOT}" "127.0.0.1:${src_port}" standalone 0 "${dst_csv}" cluster 0 "${prefix}:*"
  target_master=$(find_first_master_port "${dst_ports[@]}")
  assert_expected_cluster_state "${target_master}" "${prefix}"
  expect_absent "$(redis_call standalone "${src_port}" exists "nonbisync:cat6:s2c:isolated:${name}")" "standalone->cluster isolated target key on source"
  assert_no_bisync_metadata_cluster "${dst_ports[@]}"
  assert_checkpoint_signals_cluster "${target_master}" "${dst_ports[@]}"
  assert_log_has_no_bisync_markers "${TMP_ROOT}/${name}-s2c.log"
  assert_log_has_no_bisync_markers "${TMP_ROOT}/${name}-s2c.restart.log"
  assert_log_has_resume_signal "${TMP_ROOT}/${name}-s2c.restart.log"

  stop_pid "${SYNCER_PID}"
  SYNCER_PID=""
  shutdown_ports "${src_port}" "${dst_ports[@]}"
}

echo "[1/1] building binaries"
build_nonbisync_binaries "${TMP_ROOT}"

case ",${SCENARIOS}," in
  *",sync,"*)
    run_cluster_to_standalone sync sync
    run_standalone_to_cluster sync sync
    ;;
esac
case ",${SCENARIOS}," in
  *",pipeline,"*)
    run_cluster_to_standalone pipeline pipeline
    run_standalone_to_cluster pipeline pipeline
    ;;
esac
