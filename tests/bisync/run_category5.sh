#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_ROOT="${TMPDIR:-/tmp}/redisgunyu-bisync-cat5"
source "${ROOT}/tests/bisync/lib/redis_env.sh"
TEST_PREFIX="${TEST_PREFIX:-bisync:cat5:$(date +%s)}"
SCENARIOS="${SCENARIOS:-sync,pipeline,parallel}"
SERIAL_SRC_BASE="${SERIAL_SRC_BASE:-30300}"
SERIAL_DST_BASE="${SERIAL_DST_BASE:-30400}"
SERIAL_HTTP_PORT="${SERIAL_HTTP_PORT:-30380}"
SERIAL_REV_HTTP_PORT="${SERIAL_REV_HTTP_PORT:-30480}"
PIPELINE_SRC_BASE="${PIPELINE_SRC_BASE:-30500}"
PIPELINE_DST_BASE="${PIPELINE_DST_BASE:-30600}"
PIPELINE_FWD_HTTP_PORT="${PIPELINE_FWD_HTTP_PORT:-30580}"
PIPELINE_REV_HTTP_PORT="${PIPELINE_REV_HTTP_PORT:-30680}"
FWD_PID=""
REV_PID=""
REDIS_SERVER_BIN="$(resolve_redis_server_bin REDIS_SERVER_BIN REDIS_DEPLOY_ROOT)"

cleanup() {
  local code=$?
  set +e
  stop_syncers
  shutdown_ports $(category5_all_ports)
  if [[ "${KEEP_TMP:-0}" != "1" ]]; then
    rm -rf "${TMP_ROOT}"
  fi
  exit "${code}"
}
trap cleanup EXIT

stop_syncers() {
  if [[ -n "${FWD_PID}" ]]; then
    kill "${FWD_PID}" >/dev/null 2>&1 || true
    wait "${FWD_PID}" >/dev/null 2>&1 || true
    FWD_PID=""
  fi
  if [[ -n "${REV_PID}" ]]; then
    kill "${REV_PID}" >/dev/null 2>&1 || true
    wait "${REV_PID}" >/dev/null 2>&1 || true
    REV_PID=""
  fi
}

shutdown_ports() {
  local port
  for port in "$@"; do
    redis-cli -p "${port}" shutdown nosave >/dev/null 2>&1 || true
  done
}

category5_all_ports() {
  printf '%s\n' \
    "${SERIAL_SRC_BASE}" "$((SERIAL_SRC_BASE + 1))" "$((SERIAL_SRC_BASE + 2))" "$((SERIAL_SRC_BASE + 3))" "$((SERIAL_SRC_BASE + 4))" "$((SERIAL_SRC_BASE + 5))" \
    "${SERIAL_DST_BASE}" "$((SERIAL_DST_BASE + 1))" "$((SERIAL_DST_BASE + 2))" "$((SERIAL_DST_BASE + 3))" "$((SERIAL_DST_BASE + 4))" "$((SERIAL_DST_BASE + 5))" \
    "${PIPELINE_SRC_BASE}" "$((PIPELINE_SRC_BASE + 1))" "$((PIPELINE_SRC_BASE + 2))" "$((PIPELINE_SRC_BASE + 3))" "$((PIPELINE_SRC_BASE + 4))" "$((PIPELINE_SRC_BASE + 5))" \
    "${PIPELINE_DST_BASE}" "$((PIPELINE_DST_BASE + 1))" "$((PIPELINE_DST_BASE + 2))" "$((PIPELINE_DST_BASE + 3))" "$((PIPELINE_DST_BASE + 4))" "$((PIPELINE_DST_BASE + 5))"
}

rm -rf "${TMP_ROOT}"
mkdir -p "${TMP_ROOT}"

build_binaries() {
  echo "[1/6] building binaries"
  mkdir -p "${TMP_ROOT}/gocache"
  (cd "${ROOT}" && GOCACHE="${TMP_ROOT}/gocache" go build -o "${TMP_ROOT}/redisGunYu" ./main.go)
  (cd "${ROOT}" && GOCACHE="${TMP_ROOT}/gocache" go build -o "${TMP_ROOT}/bisync_compare" ./tests/bisync/cmd/bisync_compare)
}

run_unit_tests() {
  echo "[2/6] running topology unit tests"
  (cd "${ROOT}" && GOCACHE="${TMP_ROOT}/gocache" go test ./cmd -run 'TestTypologySuite' -count=1)
}

write_redis_conf() {
  local dir=$1
  local port=$2
  mkdir -p "${dir}"
  cat > "${dir}/redis.conf" <<EOF
port ${port}
bind 127.0.0.1
protected-mode no
daemonize yes
dir ${dir}
pidfile ${dir}/redis.pid
logfile ${dir}/redis.log
save ""
appendonly no
cluster-enabled yes
cluster-config-file nodes.conf
cluster-node-timeout 3000
EOF
}

wait_for_ping() {
  local port=$1
  for _ in $(seq 1 50); do
    if redis-cli -p "${port}" ping >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  echo "redis on port ${port} did not start" >&2
  return 1
}

wait_for_cluster_ok() {
  local port=$1
  for _ in $(seq 1 120); do
    if redis-cli -p "${port}" cluster info 2>/dev/null | rg -q '^cluster_state:ok'; then
      return 0
    fi
    sleep 0.25
  done
  echo "cluster on port ${port} did not become ready" >&2
  return 1
}

start_cluster_with_replicas() {
  local prefix=$1
  shift
  local ports=("$@")
  echo "[3/6] starting cluster ${prefix} on ports ${ports[*]}"
  local port
  for port in "${ports[@]}"; do
    write_redis_conf "${TMP_ROOT}/${prefix}-${port}" "${port}"
    "${REDIS_SERVER_BIN}" "${TMP_ROOT}/${prefix}-${port}/redis.conf"
    wait_for_ping "${port}"
  done
  redis-cli --cluster create \
    "127.0.0.1:${ports[0]}" \
    "127.0.0.1:${ports[1]}" \
    "127.0.0.1:${ports[2]}" \
    "127.0.0.1:${ports[3]}" \
    "127.0.0.1:${ports[4]}" \
    "127.0.0.1:${ports[5]}" \
    --cluster-replicas 1 \
    --cluster-yes >/dev/null
  local port
  for port in "${ports[@]}"; do
    wait_for_cluster_ok "${port}"
  done
  sleep 1
}

write_syncer_conf() {
  local name=$1
  local http_port=$2
  local input_addrs=$3
  local output_addrs=$4
  local mode_arg=$5
  local replay_mode
  replay_mode=$(normalize_replay_mode "${mode_arg}")
  local storer_dir="${TMP_ROOT}/${name}-store"
  mkdir -p "${storer_dir}"
  cat > "${TMP_ROOT}/${name}.yaml" <<EOF
server:
  listen: 127.0.0.1:${http_port}
  listenPeer: 127.0.0.1:${http_port}
  gracefullStopTimeout: 1s
  checkRedisTypologyTicker: 2s
input:
  redis:
    addresses: [${input_addrs}]
    type: cluster
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
    addresses: [${output_addrs}]
    type: cluster
    version: "7.0.11"
  replay:
    resumeFromBreakPoint: true
    keyExists: replace
    metric: false
    targetDb: -1
    replayTransaction: true
    bisyncEnabled: true
    mode: ${replay_mode}
log:
  level: info
  handler:
    stdout: true
  withCaller: false
  withFunc: false
EOF
}

wait_for_syncer() {
  local port=$1
  for _ in $(seq 1 100); do
    if curl -sf "http://127.0.0.1:${port}/syncer/status" >/dev/null; then
      return 0
    fi
    sleep 0.2
  done
  echo "syncer on http port ${port} did not become ready" >&2
  return 1
}

start_syncers() {
  local name=$1
  local src_ports_csv=$2
  local dst_ports_csv=$3
  local fwd_http_port=$4
  local rev_http_port=$5
  local mode_arg=$6
  local replay_mode
  replay_mode=$(normalize_replay_mode "${mode_arg}")

  write_syncer_conf "${name}-forward" "${fwd_http_port}" "${src_ports_csv}" "${dst_ports_csv}" "${mode_arg}"
  write_syncer_conf "${name}-reverse" "${rev_http_port}" "${dst_ports_csv}" "${src_ports_csv}" "${mode_arg}"

  "${TMP_ROOT}/redisGunYu" -conf "${TMP_ROOT}/${name}-forward.yaml" -cmd sync > "${TMP_ROOT}/${name}-forward.log" 2>&1 &
  FWD_PID=$!
  "${TMP_ROOT}/redisGunYu" -conf "${TMP_ROOT}/${name}-reverse.yaml" -cmd sync > "${TMP_ROOT}/${name}-reverse.log" 2>&1 &
  REV_PID=$!

  wait_for_syncer "${fwd_http_port}"
  wait_for_syncer "${rev_http_port}"
  sleep 2
}

restart_syncers_via_api() {
  local fwd_http_port=$1
  local rev_http_port=$2
  curl -sf -XPOST "http://127.0.0.1:${fwd_http_port}/syncer/restart" >/dev/null
  curl -sf -XPOST "http://127.0.0.1:${rev_http_port}/syncer/restart" >/dev/null
  wait_for_syncer "${fwd_http_port}"
  wait_for_syncer "${rev_http_port}"
  sleep 3
}

redis_cmd() {
  local port=$1
  shift
  redis-cli -c -p "${port}" "$@" >/dev/null
}

find_first_master_port() {
  local port
  for port in "$@"; do
    if [[ "$(redis-cli -p "${port}" --raw role 2>/dev/null | head -n 1)" == "master" ]]; then
      echo "${port}"
      return 0
    fi
  done
  return 1
}

find_first_replica_port() {
  local port
  for port in "$@"; do
    if [[ "$(redis-cli -p "${port}" --raw role 2>/dev/null | head -n 1)" == "slave" ]]; then
      echo "${port}"
      return 0
    fi
  done
  return 1
}

wait_for_role() {
  local port=$1
  local expected=$2
  for _ in $(seq 1 80); do
    if [[ "$(redis-cli -p "${port}" --raw role 2>/dev/null | head -n 1)" == "${expected}" ]]; then
      return 0
    fi
    sleep 0.25
  done
  echo "redis on port ${port} did not become ${expected}" >&2
  return 1
}

wait_for_cluster_all_ok() {
  local port
  for port in "$@"; do
    wait_for_cluster_ok "${port}"
  done
}

force_failover() {
  local replica_port=$1
  redis-cli -p "${replica_port}" cluster failover force >/dev/null
  wait_for_role "${replica_port}" master
  sleep 1
}

wait_for_converge() {
  local left_addrs=$1
  local right_addrs=$2
  local pattern=$3
  for _ in $(seq 1 80); do
    if "${TMP_ROOT}/bisync_compare" --left-addrs "${left_addrs}" --right-addrs "${right_addrs}" --pattern "${pattern}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  "${TMP_ROOT}/bisync_compare" --left-addrs "${left_addrs}" --right-addrs "${right_addrs}" --pattern "${pattern}"
}

trim_joined_lines() {
  tr '\n' ' ' | sed 's/[[:space:]]*$//'
}

set_state() {
  local port=$1
  local key=$2
  redis-cli -c -p "${port}" --raw smembers "${key}" | sort | trim_joined_lines
}

list_state() {
  local port=$1
  local key=$2
  redis-cli -c -p "${port}" --raw lrange "${key}" 0 -1 | trim_joined_lines
}

hash_state() {
  local port=$1
  local key=$2
  redis-cli -c -p "${port}" --raw hgetall "${key}" | awk 'NR % 2 == 1 { printf "%s=", $0; next } { printf "%s\n", $0 }' | sort | paste -sd'|' -
}

expect_eq() {
  local actual=$1
  local expected=$2
  local label=$3
  if [[ "${actual}" != "${expected}" ]]; then
    echo "assertion failed for ${label}: got=[${actual}] want=[${expected}]" >&2
    exit 1
  fi
}

expect_absent() {
  local actual=$1
  local label=$2
  if [[ "${actual}" != "0" ]]; then
    echo "assertion failed for ${label}: key should be absent, got exists=${actual}" >&2
    exit 1
  fi
}

left_key() {
  local prefix=$1
  local name=$2
  printf "%s:left:%s{cat5-left}" "${prefix}" "${name}"
}

right_key() {
  local prefix=$1
  local name=$2
  printf "%s:right:%s{cat5-right}" "${prefix}" "${name}"
}

write_left_phase() {
  local prefix=$1
  local port=$2
  local phase=$3
  case "${phase}" in
    1)
      redis_cmd "${port}" set "$(left_key "${prefix}" "string")" "p1"
      redis_cmd "${port}" incrby "$(left_key "${prefix}" "ctr")" 2
      redis_cmd "${port}" sadd "$(left_key "${prefix}" "set")" red blue
      redis_cmd "${port}" hset "$(left_key "${prefix}" "hash")" f1 v1
      redis_cmd "${port}" rpush "$(left_key "${prefix}" "list")" a b
      ;;
    2)
      redis_cmd "${port}" set "$(left_key "${prefix}" "string")" "p2"
      redis_cmd "${port}" incrby "$(left_key "${prefix}" "ctr")" 5
      redis_cmd "${port}" sadd "$(left_key "${prefix}" "set")" green
      redis_cmd "${port}" hset "$(left_key "${prefix}" "hash")" f2 v2
      redis_cmd "${port}" rpush "$(left_key "${prefix}" "list")" c d
      ;;
    3)
      redis_cmd "${port}" set "$(left_key "${prefix}" "string")" "p3"
      redis_cmd "${port}" incrby "$(left_key "${prefix}" "ctr")" 7
      redis_cmd "${port}" sadd "$(left_key "${prefix}" "set")" yellow
      redis_cmd "${port}" hset "$(left_key "${prefix}" "hash")" f3 v3
      redis_cmd "${port}" rpush "$(left_key "${prefix}" "list")" e f
      ;;
    4)
      redis_cmd "${port}" set "$(left_key "${prefix}" "string")" "p4"
      redis_cmd "${port}" incrby "$(left_key "${prefix}" "ctr")" 11
      redis_cmd "${port}" sadd "$(left_key "${prefix}" "set")" white
      redis_cmd "${port}" hset "$(left_key "${prefix}" "hash")" f4 v4
      redis_cmd "${port}" del "$(left_key "${prefix}" "gone")"
      redis_cmd "${port}" rpush "$(left_key "${prefix}" "list")" g h
      ;;
  esac
}

write_right_phase() {
  local prefix=$1
  local port=$2
  local phase=$3
  case "${phase}" in
    1)
      redis_cmd "${port}" set "$(right_key "${prefix}" "string")" "q1"
      redis_cmd "${port}" incrby "$(right_key "${prefix}" "ctr")" 3
      redis_cmd "${port}" sadd "$(right_key "${prefix}" "set")" east south
      redis_cmd "${port}" hset "$(right_key "${prefix}" "hash")" g1 w1
      redis_cmd "${port}" rpush "$(right_key "${prefix}" "list")" m n
      ;;
    2)
      redis_cmd "${port}" set "$(right_key "${prefix}" "string")" "q2"
      redis_cmd "${port}" incrby "$(right_key "${prefix}" "ctr")" 6
      redis_cmd "${port}" sadd "$(right_key "${prefix}" "set")" west
      redis_cmd "${port}" hset "$(right_key "${prefix}" "hash")" g2 w2
      redis_cmd "${port}" rpush "$(right_key "${prefix}" "list")" o p
      ;;
    3)
      redis_cmd "${port}" set "$(right_key "${prefix}" "string")" "q3"
      redis_cmd "${port}" incrby "$(right_key "${prefix}" "ctr")" 8
      redis_cmd "${port}" sadd "$(right_key "${prefix}" "set")" north
      redis_cmd "${port}" hset "$(right_key "${prefix}" "hash")" g3 w3
      redis_cmd "${port}" rpush "$(right_key "${prefix}" "list")" q r
      ;;
    4)
      redis_cmd "${port}" set "$(right_key "${prefix}" "string")" "q4"
      redis_cmd "${port}" incrby "$(right_key "${prefix}" "ctr")" 12
      redis_cmd "${port}" sadd "$(right_key "${prefix}" "set")" center
      redis_cmd "${port}" hset "$(right_key "${prefix}" "hash")" g4 w4
      redis_cmd "${port}" del "$(right_key "${prefix}" "gone")"
      redis_cmd "${port}" rpush "$(right_key "${prefix}" "list")" s t
      ;;
  esac
}

seed_initial_keys() {
  local prefix=$1
  local left_port=$2
  local right_port=$3
  redis_cmd "${left_port}" set "$(left_key "${prefix}" "gone")" "gone-left"
  redis_cmd "${right_port}" set "$(right_key "${prefix}" "gone")" "gone-right"
}

assert_expected_state() {
  local port=$1
  local prefix=$2
  expect_eq "$(redis-cli -c -p "${port}" --raw get "$(left_key "${prefix}" "string")")" "p4" "$(left_key "${prefix}" "string")"
  expect_eq "$(redis-cli -c -p "${port}" --raw get "$(left_key "${prefix}" "ctr")")" "25" "$(left_key "${prefix}" "ctr")"
  expect_eq "$(set_state "${port}" "$(left_key "${prefix}" "set")")" "blue green red white yellow" "$(left_key "${prefix}" "set")"
  expect_eq "$(hash_state "${port}" "$(left_key "${prefix}" "hash")")" "f1=v1|f2=v2|f3=v3|f4=v4" "$(left_key "${prefix}" "hash")"
  expect_eq "$(list_state "${port}" "$(left_key "${prefix}" "list")")" "a b c d e f g h" "$(left_key "${prefix}" "list")"
  expect_absent "$(redis-cli -c -p "${port}" --raw exists "$(left_key "${prefix}" "gone")")" "$(left_key "${prefix}" "gone")"

  expect_eq "$(redis-cli -c -p "${port}" --raw get "$(right_key "${prefix}" "string")")" "q4" "$(right_key "${prefix}" "string")"
  expect_eq "$(redis-cli -c -p "${port}" --raw get "$(right_key "${prefix}" "ctr")")" "29" "$(right_key "${prefix}" "ctr")"
  expect_eq "$(set_state "${port}" "$(right_key "${prefix}" "set")")" "center east north south west" "$(right_key "${prefix}" "set")"
  expect_eq "$(hash_state "${port}" "$(right_key "${prefix}" "hash")")" "g1=w1|g2=w2|g3=w3|g4=w4" "$(right_key "${prefix}" "hash")"
  expect_eq "$(list_state "${port}" "$(right_key "${prefix}" "list")")" "m n o p q r s t" "$(right_key "${prefix}" "list")"
  expect_absent "$(redis-cli -c -p "${port}" --raw exists "$(right_key "${prefix}" "gone")")" "$(right_key "${prefix}" "gone")"
}

assert_log_indicates_restart() {
  local log_file=$1
  if ! rg -q 'restart|typology|redis typology is changed|run error' "${log_file}"; then
    echo "expected restart/topology activity in ${log_file}" >&2
    exit 1
  fi
}

run_scenario() {
  local mode=$1
  local mode_arg=$2
  local replay_mode
  replay_mode=$(normalize_replay_mode "${mode_arg}")
  local prefix="${TEST_PREFIX}:${mode}"
  local src_ports
  local dst_ports
  local fwd_http_port
  local rev_http_port
  local src_csv
  local dst_csv
  local source_master_port
  local target_master_port
  local source_replica_port
  local target_replica_port

  if ! replay_mode_uses_frontier "${replay_mode}"; then
    src_ports=("${SERIAL_SRC_BASE}" "$((SERIAL_SRC_BASE + 1))" "$((SERIAL_SRC_BASE + 2))" "$((SERIAL_SRC_BASE + 3))" "$((SERIAL_SRC_BASE + 4))" "$((SERIAL_SRC_BASE + 5))")
    dst_ports=("${SERIAL_DST_BASE}" "$((SERIAL_DST_BASE + 1))" "$((SERIAL_DST_BASE + 2))" "$((SERIAL_DST_BASE + 3))" "$((SERIAL_DST_BASE + 4))" "$((SERIAL_DST_BASE + 5))")
    fwd_http_port="${SERIAL_HTTP_PORT}"
    rev_http_port="${SERIAL_REV_HTTP_PORT}"
  else
    src_ports=("${PIPELINE_SRC_BASE}" "$((PIPELINE_SRC_BASE + 1))" "$((PIPELINE_SRC_BASE + 2))" "$((PIPELINE_SRC_BASE + 3))" "$((PIPELINE_SRC_BASE + 4))" "$((PIPELINE_SRC_BASE + 5))")
    dst_ports=("${PIPELINE_DST_BASE}" "$((PIPELINE_DST_BASE + 1))" "$((PIPELINE_DST_BASE + 2))" "$((PIPELINE_DST_BASE + 3))" "$((PIPELINE_DST_BASE + 4))" "$((PIPELINE_DST_BASE + 5))")
    fwd_http_port="${PIPELINE_FWD_HTTP_PORT}"
    rev_http_port="${PIPELINE_REV_HTTP_PORT}"
  fi

  src_csv=$(printf "127.0.0.1:%s,127.0.0.1:%s,127.0.0.1:%s,127.0.0.1:%s,127.0.0.1:%s,127.0.0.1:%s" "${src_ports[0]}" "${src_ports[1]}" "${src_ports[2]}" "${src_ports[3]}" "${src_ports[4]}" "${src_ports[5]}")
  dst_csv=$(printf "127.0.0.1:%s,127.0.0.1:%s,127.0.0.1:%s,127.0.0.1:%s,127.0.0.1:%s,127.0.0.1:%s" "${dst_ports[0]}" "${dst_ports[1]}" "${dst_ports[2]}" "${dst_ports[3]}" "${dst_ports[4]}" "${dst_ports[5]}")

  start_cluster_with_replicas "${mode}-src" "${src_ports[@]}"
  start_cluster_with_replicas "${mode}-dst" "${dst_ports[@]}"
  start_syncers "${mode}" "${src_csv}" "${dst_csv}" "${fwd_http_port}" "${rev_http_port}" "${mode_arg}"

  echo "[4/6] scenario ${mode}: initial bidirectional sync"
  source_master_port=$(find_first_master_port "${src_ports[@]}")
  target_master_port=$(find_first_master_port "${dst_ports[@]}")
  seed_initial_keys "${prefix}" "${source_master_port}" "${target_master_port}"
  write_left_phase "${prefix}" "${source_master_port}" 1
  write_right_phase "${prefix}" "${target_master_port}" 1
  wait_for_converge "${src_csv}" "${dst_csv}" "${prefix}:*"

  echo "[5/6] scenario ${mode}: source failover, target failover, syncer restart"
  source_replica_port=$(find_first_replica_port "${src_ports[@]}")
  force_failover "${source_replica_port}"
  wait_for_cluster_all_ok "${src_ports[@]}"
  source_master_port=$(find_first_master_port "${src_ports[@]}")
  target_master_port=$(find_first_master_port "${dst_ports[@]}")
  write_left_phase "${prefix}" "${source_master_port}" 2
  write_right_phase "${prefix}" "${target_master_port}" 2
  wait_for_converge "${src_csv}" "${dst_csv}" "${prefix}:*"

  target_replica_port=$(find_first_replica_port "${dst_ports[@]}")
  force_failover "${target_replica_port}"
  wait_for_cluster_all_ok "${dst_ports[@]}"
  source_master_port=$(find_first_master_port "${src_ports[@]}")
  target_master_port=$(find_first_master_port "${dst_ports[@]}")
  write_left_phase "${prefix}" "${source_master_port}" 3
  write_right_phase "${prefix}" "${target_master_port}" 3
  wait_for_converge "${src_csv}" "${dst_csv}" "${prefix}:*"

  restart_syncers_via_api "${fwd_http_port}" "${rev_http_port}"
  source_master_port=$(find_first_master_port "${src_ports[@]}")
  target_master_port=$(find_first_master_port "${dst_ports[@]}")
  write_left_phase "${prefix}" "${source_master_port}" 4
  write_right_phase "${prefix}" "${target_master_port}" 4
  wait_for_converge "${src_csv}" "${dst_csv}" "${prefix}:*"
  "${TMP_ROOT}/bisync_compare" --left-addrs "${src_csv}" --right-addrs "${dst_csv}" --pattern "${prefix}:*"

  source_master_port=$(find_first_master_port "${src_ports[@]}")
  target_master_port=$(find_first_master_port "${dst_ports[@]}")
  assert_expected_state "${source_master_port}" "${prefix}"
  assert_expected_state "${target_master_port}" "${prefix}"
  assert_log_indicates_restart "${TMP_ROOT}/${mode}-forward.log"
  assert_log_indicates_restart "${TMP_ROOT}/${mode}-reverse.log"

  echo "[6/6] scenario ${mode}: summary"
  echo "prefix=${prefix}"
  echo "mode=${replay_mode}"
  echo "source_replica_failover=${source_replica_port}"
  echo "target_replica_failover=${target_replica_port}"
  echo "forward_log=${TMP_ROOT}/${mode}-forward.log"
  echo "reverse_log=${TMP_ROOT}/${mode}-reverse.log"

  stop_syncers
  shutdown_ports "${src_ports[@]}" "${dst_ports[@]}"
}

build_binaries
run_unit_tests

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
case ",${SCENARIOS}," in
  *",parallel,"*)
    run_scenario parallel parallel
    ;;
esac
