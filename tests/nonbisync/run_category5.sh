#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_ROOT="${TMPDIR:-/tmp}/redisgunyu-nonbisync-cat5"
source "${ROOT}/tests/nonbisync/lib/test_env.sh"

SCENARIOS="${SCENARIOS:-sync,pipeline}"
SYNC_SRC_BASE="${SYNC_SRC_BASE:-32700}"
SYNC_DST_BASE="${SYNC_DST_BASE:-32800}"
SYNC_HTTP_PORT="${SYNC_HTTP_PORT:-32780}"
PIPE_SRC_BASE="${PIPE_SRC_BASE:-32900}"
PIPE_DST_BASE="${PIPE_DST_BASE:-33000}"
PIPE_HTTP_PORT="${PIPE_HTTP_PORT:-32980}"
TEST_PREFIX="${TEST_PREFIX:-nonbisync:cat5:$(date +%s)}"
SYNCER_PID=""
REDIS_SERVER_BIN="$(resolve_redis_server_bin REDIS_SERVER_BIN REDIS_DEPLOY_ROOT)"

cleanup() {
  local code=$?
  set +e
  stop_pid "${SYNCER_PID:-}"
  shutdown_ports \
    "${SYNC_SRC_BASE}" "$((SYNC_SRC_BASE + 1))" "$((SYNC_SRC_BASE + 2))" "$((SYNC_SRC_BASE + 3))" "$((SYNC_SRC_BASE + 4))" "$((SYNC_SRC_BASE + 5))" \
    "${SYNC_DST_BASE}" "$((SYNC_DST_BASE + 1))" "$((SYNC_DST_BASE + 2))" "$((SYNC_DST_BASE + 3))" "$((SYNC_DST_BASE + 4))" "$((SYNC_DST_BASE + 5))" \
    "${PIPE_SRC_BASE}" "$((PIPE_SRC_BASE + 1))" "$((PIPE_SRC_BASE + 2))" "$((PIPE_SRC_BASE + 3))" "$((PIPE_SRC_BASE + 4))" "$((PIPE_SRC_BASE + 5))" \
    "${PIPE_DST_BASE}" "$((PIPE_DST_BASE + 1))" "$((PIPE_DST_BASE + 2))" "$((PIPE_DST_BASE + 3))" "$((PIPE_DST_BASE + 4))" "$((PIPE_DST_BASE + 5))"
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
      redis_cmd cluster "${port}" set "$(key_name "${prefix}" "string")" "p1"
      redis_cmd cluster "${port}" incrby "$(key_name "${prefix}" "counter")" 1
      redis_cmd cluster "${port}" sadd "$(key_name "${prefix}" "set")" red
      redis_cmd cluster "${port}" hset "$(key_name "${prefix}" "hash")" f1 v1 state p1
      redis_cmd cluster "${port}" rpush "$(key_name "${prefix}" "list")" a
      redis_cmd cluster "${port}" zadd "$(key_name "${prefix}" "zset")" 1 one
      redis-cli -c -p "${port}" <<EOF >/dev/null
MULTI
INCRBY $(key_name "${prefix}" "txn-counter{txn}") 1
RPUSH $(key_name "${prefix}" "txn-list{txn}") p1
HSET $(key_name "${prefix}" "txn-hash{txn}") phase p1
EXEC
EOF
      ;;
    2)
      redis_cmd cluster "${port}" set "$(key_name "${prefix}" "string")" "p2"
      redis_cmd cluster "${port}" incrby "$(key_name "${prefix}" "counter")" 2
      redis_cmd cluster "${port}" sadd "$(key_name "${prefix}" "set")" blue
      redis_cmd cluster "${port}" hset "$(key_name "${prefix}" "hash")" f2 v2 state p2
      redis_cmd cluster "${port}" rpush "$(key_name "${prefix}" "list")" b c
      redis_cmd cluster "${port}" zadd "$(key_name "${prefix}" "zset")" 2 two
      redis-cli -c -p "${port}" <<EOF >/dev/null
MULTI
INCRBY $(key_name "${prefix}" "txn-counter{txn}") 2
RPUSH $(key_name "${prefix}" "txn-list{txn}") p2
HSET $(key_name "${prefix}" "txn-hash{txn}") phase p2 f2 v2
EXEC
EOF
      ;;
    3)
      redis_cmd cluster "${port}" set "$(key_name "${prefix}" "string")" "p3"
      redis_cmd cluster "${port}" incrby "$(key_name "${prefix}" "counter")" 3
      redis_cmd cluster "${port}" sadd "$(key_name "${prefix}" "set")" green
      redis_cmd cluster "${port}" hset "$(key_name "${prefix}" "hash")" f3 v3 state p3
      redis_cmd cluster "${port}" rpush "$(key_name "${prefix}" "list")" d
      redis_cmd cluster "${port}" zadd "$(key_name "${prefix}" "zset")" 3 three
      redis-cli -c -p "${port}" <<EOF >/dev/null
MULTI
INCRBY $(key_name "${prefix}" "txn-counter{txn}") 3
RPUSH $(key_name "${prefix}" "txn-list{txn}") p3
HSET $(key_name "${prefix}" "txn-hash{txn}") phase p3 f3 v3
EXEC
EOF
      ;;
    4)
      redis_cmd cluster "${port}" set "$(key_name "${prefix}" "string")" "p4"
      redis_cmd cluster "${port}" incrby "$(key_name "${prefix}" "counter")" 5
      redis_cmd cluster "${port}" sadd "$(key_name "${prefix}" "set")" yellow
      redis_cmd cluster "${port}" hset "$(key_name "${prefix}" "hash")" f4 v4 state p4
      redis_cmd cluster "${port}" rpush "$(key_name "${prefix}" "list")" e f
      redis_cmd cluster "${port}" zadd "$(key_name "${prefix}" "zset")" 4 four
      redis_cmd cluster "${port}" set "$(key_name "${prefix}" "delete-me")" "gone"
      redis_cmd cluster "${port}" del "$(key_name "${prefix}" "delete-me")"
      redis-cli -c -p "${port}" <<EOF >/dev/null
MULTI
INCRBY $(key_name "${prefix}" "txn-counter{txn}") 4
RPUSH $(key_name "${prefix}" "txn-list{txn}") p4
HSET $(key_name "${prefix}" "txn-hash{txn}") phase p4 f4 v4
EXEC
EOF
      ;;
    5)
      redis_cmd cluster "${port}" set "$(key_name "${prefix}" "string")" "p5"
      redis_cmd cluster "${port}" incrby "$(key_name "${prefix}" "counter")" 7
      redis_cmd cluster "${port}" sadd "$(key_name "${prefix}" "set")" white
      redis_cmd cluster "${port}" hset "$(key_name "${prefix}" "hash")" f5 v5 state p5
      redis_cmd cluster "${port}" rpush "$(key_name "${prefix}" "list")" g
      redis_cmd cluster "${port}" zadd "$(key_name "${prefix}" "zset")" 5 five
      redis-cli -c -p "${port}" <<EOF >/dev/null
MULTI
INCRBY $(key_name "${prefix}" "txn-counter{txn}") 5
RPUSH $(key_name "${prefix}" "txn-list{txn}") p5
HSET $(key_name "${prefix}" "txn-hash{txn}") phase p5 f5 v5
EXEC
EOF
      ;;
  esac
}

assert_expected_state() {
  local port=$1
  local prefix=$2

  expect_eq "$(redis_call cluster "${port}" get "$(key_name "${prefix}" "string")")" "p5" "$(key_name "${prefix}" "string")"
  expect_eq "$(redis_call cluster "${port}" get "$(key_name "${prefix}" "counter")")" "18" "$(key_name "${prefix}" "counter")"
  expect_eq "$(set_state cluster "${port}" "$(key_name "${prefix}" "set")")" "blue green red white yellow" "$(key_name "${prefix}" "set")"
  expect_eq "$(hash_state cluster "${port}" "$(key_name "${prefix}" "hash")")" "f1=v1|f2=v2|f3=v3|f4=v4|f5=v5|state=p5" "$(key_name "${prefix}" "hash")"
  expect_eq "$(list_state cluster "${port}" "$(key_name "${prefix}" "list")")" "a b c d e f g" "$(key_name "${prefix}" "list")"
  expect_eq "$(zset_state cluster "${port}" "$(key_name "${prefix}" "zset")")" "one=1|two=2|three=3|four=4|five=5" "$(key_name "${prefix}" "zset")"
  expect_eq "$(redis_call cluster "${port}" get "$(key_name "${prefix}" "txn-counter{txn}")")" "15" "$(key_name "${prefix}" "txn-counter{txn}")"
  expect_eq "$(list_state cluster "${port}" "$(key_name "${prefix}" "txn-list{txn}")")" "p1 p2 p3 p4 p5" "$(key_name "${prefix}" "txn-list{txn}")"
  expect_eq "$(hash_state cluster "${port}" "$(key_name "${prefix}" "txn-hash{txn}")")" "f2=v2|f3=v3|f4=v4|f5=v5|phase=p5" "$(key_name "${prefix}" "txn-hash{txn}")"
  expect_absent "$(redis_call cluster "${port}" exists "$(key_name "${prefix}" "delete-me")")" "$(key_name "${prefix}" "delete-me")"
}

run_scenario() {
  local name=$1
  local mode=$2
  local src_base dst_base http_port
  local src_ports dst_ports src_csv dst_csv prefix source_master source_replica target_replica target_master conf_file

  if [[ "${mode}" == "sync" ]]; then
    src_base=${SYNC_SRC_BASE}
    dst_base=${SYNC_DST_BASE}
    http_port=${SYNC_HTTP_PORT}
  else
    src_base=${PIPE_SRC_BASE}
    dst_base=${PIPE_DST_BASE}
    http_port=${PIPE_HTTP_PORT}
  fi

  src_ports=("${src_base}" "$((src_base + 1))" "$((src_base + 2))" "$((src_base + 3))" "$((src_base + 4))" "$((src_base + 5))")
  dst_ports=("${dst_base}" "$((dst_base + 1))" "$((dst_base + 2))" "$((dst_base + 3))" "$((dst_base + 4))" "$((dst_base + 5))")
  src_csv=$(format_addrs "${src_ports[@]}")
  dst_csv=$(format_addrs "${dst_ports[@]}")
  prefix="${TEST_PREFIX}:${name}"

  echo "[category5] scenario=${name} mode=${mode}"
  start_cluster_with_replicas "${REDIS_SERVER_BIN}" "${TMP_ROOT}" "${name}-src" "${src_ports[@]}"
  start_cluster_with_replicas "${REDIS_SERVER_BIN}" "${TMP_ROOT}" "${name}-dst" "${dst_ports[@]}"

  conf_file="${TMP_ROOT}/${name}.yaml"
  write_syncer_conf "${conf_file}" "${http_port}" "${src_csv}" cluster "${dst_csv}" cluster "${TMP_ROOT}/${name}-store" "${mode}"
  SYNCER_PID=$(start_syncer_process "${TMP_ROOT}" "${conf_file}" "${TMP_ROOT}/${name}.log")
  wait_for_syncer "${http_port}"
  sleep 2

  source_master=$(find_first_master_port "${src_ports[@]}")
  target_master=$(find_first_master_port "${dst_ports[@]}")
  write_phase "${source_master}" "${prefix}" 1
  wait_for_cluster_equal "${TMP_ROOT}" "${src_csv}" "${dst_csv}" "${prefix}:*"

  source_replica=$(find_first_replica_port "${src_ports[@]}")
  force_failover "${source_replica}"
  for port in "${src_ports[@]}"; do
    wait_for_cluster_ok "${port}"
  done
  source_master=$(find_first_master_port "${src_ports[@]}")
  write_phase "${source_master}" "${prefix}" 2
  wait_for_cluster_equal "${TMP_ROOT}" "${src_csv}" "${dst_csv}" "${prefix}:*"

  target_replica=$(find_first_replica_port "${dst_ports[@]}")
  force_failover "${target_replica}"
  for port in "${dst_ports[@]}"; do
    wait_for_cluster_ok "${port}"
  done
  source_master=$(find_first_master_port "${src_ports[@]}")
  write_phase "${source_master}" "${prefix}" 3
  wait_for_cluster_equal "${TMP_ROOT}" "${src_csv}" "${dst_csv}" "${prefix}:*"

  stop_pid "${SYNCER_PID}"
  SYNCER_PID=""

  source_master=$(find_first_master_port "${src_ports[@]}")
  target_master=$(find_first_master_port "${dst_ports[@]}")
  write_phase "${source_master}" "${prefix}" 4
  redis_cmd cluster "${target_master}" set "nonbisync:cat5:isolated:${name}" "target-only"

  SYNCER_PID=$(start_syncer_process "${TMP_ROOT}" "${conf_file}" "${TMP_ROOT}/${name}.restart.log")
  wait_for_syncer "${http_port}"
  sleep 2

  source_master=$(find_first_master_port "${src_ports[@]}")
  write_phase "${source_master}" "${prefix}" 5
  wait_for_cluster_equal "${TMP_ROOT}" "${src_csv}" "${dst_csv}" "${prefix}:*"
  cluster_compare "${TMP_ROOT}" "${src_csv}" "${dst_csv}" "${prefix}:*"

  target_master=$(find_first_master_port "${dst_ports[@]}")
  source_master=$(find_first_master_port "${src_ports[@]}")
  assert_expected_state "${target_master}" "${prefix}"
  expect_absent "$(redis_call cluster "${source_master}" exists "nonbisync:cat5:isolated:${name}")" "isolated target key on source"
  assert_no_bisync_metadata_cluster "${dst_ports[@]}"
  assert_checkpoint_signals_cluster "${target_master}" "${dst_ports[@]}"
  assert_log_has_no_bisync_markers "${TMP_ROOT}/${name}.log"
  assert_log_has_no_bisync_markers "${TMP_ROOT}/${name}.restart.log"
  assert_log_has_topology_signal "${TMP_ROOT}/${name}.log"
  assert_log_has_resume_signal "${TMP_ROOT}/${name}.restart.log"

  stop_pid "${SYNCER_PID}"
  SYNCER_PID=""
  shutdown_ports "${src_ports[@]}" "${dst_ports[@]}"

  echo "prefix=${prefix}"
  echo "restart_log=${TMP_ROOT}/${name}.restart.log"
  echo "source_failover=${source_replica}"
  echo "target_failover=${target_replica}"
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
