#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_ROOT="${TMPDIR:-/tmp}/redisgunyu-bisync-cat2"
source "${ROOT}/tests/bisync/lib/redis_env.sh"
require_test_commands go redis-server redis-cli curl
TEST_PREFIX="${TEST_PREFIX:-bisync:cat2:$(date +%s)}"
SCENARIOS="${SCENARIOS:-sync,pipeline,parallel}"
SERIAL_SRC_BASE="${SERIAL_SRC_BASE:-29300}"
SERIAL_DST_BASE="${SERIAL_DST_BASE:-29400}"
SERIAL_FWD_HTTP_PORT="${SERIAL_FWD_HTTP_PORT:-29380}"
SERIAL_REV_HTTP_PORT="${SERIAL_REV_HTTP_PORT:-29480}"
ORDERED_SRC_BASE="${ORDERED_SRC_BASE:-29500}"
ORDERED_DST_BASE="${ORDERED_DST_BASE:-29600}"
ORDERED_FWD_HTTP_PORT="${ORDERED_FWD_HTTP_PORT:-29580}"
ORDERED_REV_HTTP_PORT="${ORDERED_REV_HTTP_PORT:-29680}"
PIPELINE_SRC_BASE="${PIPELINE_SRC_BASE:-29700}"
PIPELINE_DST_BASE="${PIPELINE_DST_BASE:-29800}"
PIPELINE_FWD_HTTP_PORT="${PIPELINE_FWD_HTTP_PORT:-29780}"
PIPELINE_REV_HTTP_PORT="${PIPELINE_REV_HTTP_PORT:-29880}"
FWD_PID=""
REV_PID=""
REDIS_SERVER_BIN="$(resolve_redis_server_bin REDIS_SERVER_BIN REDIS_DEPLOY_ROOT)"

cleanup() {
  local code=$?
  set +e
  if [[ "${KEEP_SERVERS:-0}" != "1" ]]; then
    stop_syncers
    shutdown_ports $(category2_all_ports)
  fi
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

category2_all_ports() {
  printf '%s\n' \
    "${SERIAL_SRC_BASE}" "$((SERIAL_SRC_BASE + 1))" "$((SERIAL_SRC_BASE + 2))" \
    "${SERIAL_DST_BASE}" "$((SERIAL_DST_BASE + 1))" "$((SERIAL_DST_BASE + 2))" \
    "${ORDERED_SRC_BASE}" "$((ORDERED_SRC_BASE + 1))" "$((ORDERED_SRC_BASE + 2))" \
    "${ORDERED_DST_BASE}" "$((ORDERED_DST_BASE + 1))" "$((ORDERED_DST_BASE + 2))" \
    "${PIPELINE_SRC_BASE}" "$((PIPELINE_SRC_BASE + 1))" "$((PIPELINE_SRC_BASE + 2))" \
    "${PIPELINE_DST_BASE}" "$((PIPELINE_DST_BASE + 1))" "$((PIPELINE_DST_BASE + 2))"
}

rm -rf "${TMP_ROOT}"
mkdir -p "${TMP_ROOT}"

build_binaries() {
  echo "[1/6] building binaries"
  mkdir -p "${TMP_ROOT}/gocache"
  (cd "${ROOT}" && GOCACHE="${TMP_ROOT}/gocache" go build -o "${TMP_ROOT}/redisGunYu" ./main.go)
  (cd "${ROOT}" && GOCACHE="${TMP_ROOT}/gocache" go build -o "${TMP_ROOT}/bisync_compare" ./tests/bisync/cmd/bisync_compare)
}

run_checkpoint_namespace_tests() {
  echo "[2/6] running bisync checkpoint namespace tests"
  (cd "${ROOT}" && GOCACHE="${TMP_ROOT}/gocache" go test ./syncer -run 'TestResolveBisyncCheckpointNameMigratesSerialSeedToCurrentRunID|TestResolveBisyncCheckpointNameMigratesPipelineSeedToCurrentRunID|TestResolveBisyncCheckpointNameRejectsPlainCheckpointFallback|TestRedisOutputStartPointBisyncReturnsInitialWhenOnlyPlainCheckpointExists' -count=1)
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
  for _ in $(seq 1 100); do
    if redis-cli -p "${port}" cluster info 2>/dev/null | match_regex_quiet '^cluster_state:ok'; then
      return 0
    fi
    sleep 0.2
  done
  echo "cluster on port ${port} did not become ready" >&2
  return 1
}

start_cluster() {
  local prefix=$1
  shift
  local ports=("$@")
  echo "[2/6] starting cluster ${prefix} on ports ${ports[*]}"
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
    --cluster-replicas 0 \
    --cluster-yes >/dev/null
  wait_for_cluster_ok "${ports[0]}"
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

redis_cmd() {
  local port=$1
  shift
  redis-cli -c -p "${port}" "$@" >/dev/null
}

left_key() {
  local prefix=$1
  local name=$2
  printf "%s:left:%s{left-cat2}" "${prefix}" "${name}"
}

right_key() {
  local prefix=$1
  local name=$2
  printf "%s:right:%s{right-cat2}" "${prefix}" "${name}"
}

write_phase1() {
  local prefix=$1
  local src_port=$2
  local dst_port=$3

  redis_cmd "${src_port}" set "$(left_key "${prefix}" "string")" "s1"
  redis_cmd "${src_port}" incrby "$(left_key "${prefix}" "ctr")" 2
  redis_cmd "${src_port}" sadd "$(left_key "${prefix}" "set")" red blue
  redis_cmd "${src_port}" set "$(left_key "${prefix}" "gone")" "gone-left"
  redis_cmd "${src_port}" rpush "$(left_key "${prefix}" "txn-list")" a b
  redis_cmd "${src_port}" hset "$(left_key "${prefix}" "txn-hash")" f1 v1
  redis_cmd "${src_port}" incrby "$(left_key "${prefix}" "txn-ctr")" 3

  redis_cmd "${dst_port}" set "$(right_key "${prefix}" "string")" "t1"
  redis_cmd "${dst_port}" incrby "$(right_key "${prefix}" "ctr")" 11
  redis_cmd "${dst_port}" sadd "$(right_key "${prefix}" "set")" east south
  redis_cmd "${dst_port}" set "$(right_key "${prefix}" "gone")" "gone-right"
  redis_cmd "${dst_port}" rpush "$(right_key "${prefix}" "txn-list")" x y
  redis_cmd "${dst_port}" hset "$(right_key "${prefix}" "txn-hash")" g1 w1
  redis_cmd "${dst_port}" incrby "$(right_key "${prefix}" "txn-ctr")" 6
}

write_phase2() {
  local prefix=$1
  local src_port=$2
  local dst_port=$3

  redis_cmd "${src_port}" set "$(left_key "${prefix}" "string")" "s2"
  redis_cmd "${src_port}" incrby "$(left_key "${prefix}" "ctr")" 5
  redis_cmd "${src_port}" sadd "$(left_key "${prefix}" "set")" green
  redis_cmd "${src_port}" del "$(left_key "${prefix}" "gone")"
  redis_cmd "${src_port}" set "$(left_key "${prefix}" "phase2-only")" "late-left"
  redis_cmd "${src_port}" rpush "$(left_key "${prefix}" "txn-list")" c d
  redis_cmd "${src_port}" hset "$(left_key "${prefix}" "txn-hash")" f2 v2
  redis_cmd "${src_port}" incrby "$(left_key "${prefix}" "txn-ctr")" 7

  redis_cmd "${dst_port}" set "$(right_key "${prefix}" "string")" "t2"
  redis_cmd "${dst_port}" incrby "$(right_key "${prefix}" "ctr")" 4
  redis_cmd "${dst_port}" sadd "$(right_key "${prefix}" "set")" west
  redis_cmd "${dst_port}" del "$(right_key "${prefix}" "gone")"
  redis_cmd "${dst_port}" set "$(right_key "${prefix}" "phase2-only")" "late-right"
  redis_cmd "${dst_port}" rpush "$(right_key "${prefix}" "txn-list")" z u
  redis_cmd "${dst_port}" hset "$(right_key "${prefix}" "txn-hash")" g2 w2
  redis_cmd "${dst_port}" incrby "$(right_key "${prefix}" "txn-ctr")" 9
}

write_resume_seed() {
  local prefix=$1
  local src_port=$2
  local dst_port=$3

  redis_cmd "${src_port}" set "$(left_key "${prefix}" "resume-seed")" "warm-left"
  redis_cmd "${dst_port}" set "$(right_key "${prefix}" "resume-seed")" "warm-right"
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

assert_expected_state() {
  local port=$1
  local prefix=$2

  expect_eq "$(redis-cli -c -p "${port}" --raw get "$(left_key "${prefix}" "string")")" "s2" "$(left_key "${prefix}" "string")"
  expect_eq "$(redis-cli -c -p "${port}" --raw get "$(left_key "${prefix}" "ctr")")" "7" "$(left_key "${prefix}" "ctr")"
  expect_eq "$(set_state "${port}" "$(left_key "${prefix}" "set")")" "blue green red" "$(left_key "${prefix}" "set")"
  expect_eq "$(list_state "${port}" "$(left_key "${prefix}" "txn-list")")" "a b c d" "$(left_key "${prefix}" "txn-list")"
  expect_eq "$(hash_state "${port}" "$(left_key "${prefix}" "txn-hash")")" "f1=v1|f2=v2" "$(left_key "${prefix}" "txn-hash")"
  expect_eq "$(redis-cli -c -p "${port}" --raw get "$(left_key "${prefix}" "txn-ctr")")" "10" "$(left_key "${prefix}" "txn-ctr")"
  expect_eq "$(redis-cli -c -p "${port}" --raw get "$(left_key "${prefix}" "phase2-only")")" "late-left" "$(left_key "${prefix}" "phase2-only")"
  expect_eq "$(redis-cli -c -p "${port}" --raw get "$(left_key "${prefix}" "resume-seed")")" "warm-left" "$(left_key "${prefix}" "resume-seed")"
  expect_absent "$(redis-cli -c -p "${port}" --raw exists "$(left_key "${prefix}" "gone")")" "$(left_key "${prefix}" "gone")"

  expect_eq "$(redis-cli -c -p "${port}" --raw get "$(right_key "${prefix}" "string")")" "t2" "$(right_key "${prefix}" "string")"
  expect_eq "$(redis-cli -c -p "${port}" --raw get "$(right_key "${prefix}" "ctr")")" "15" "$(right_key "${prefix}" "ctr")"
  expect_eq "$(set_state "${port}" "$(right_key "${prefix}" "set")")" "east south west" "$(right_key "${prefix}" "set")"
  expect_eq "$(list_state "${port}" "$(right_key "${prefix}" "txn-list")")" "x y z u" "$(right_key "${prefix}" "txn-list")"
  expect_eq "$(hash_state "${port}" "$(right_key "${prefix}" "txn-hash")")" "g1=w1|g2=w2" "$(right_key "${prefix}" "txn-hash")"
  expect_eq "$(redis-cli -c -p "${port}" --raw get "$(right_key "${prefix}" "txn-ctr")")" "15" "$(right_key "${prefix}" "txn-ctr")"
  expect_eq "$(redis-cli -c -p "${port}" --raw get "$(right_key "${prefix}" "phase2-only")")" "late-right" "$(right_key "${prefix}" "phase2-only")"
  expect_eq "$(redis-cli -c -p "${port}" --raw get "$(right_key "${prefix}" "resume-seed")")" "warm-right" "$(right_key "${prefix}" "resume-seed")"
  expect_absent "$(redis-cli -c -p "${port}" --raw exists "$(right_key "${prefix}" "gone")")" "$(right_key "${prefix}" "gone")"
}

cluster_scan_count() {
  local pattern=$1
  shift
  local port
  for port in "$@"; do
    redis-cli -p "${port}" --scan --pattern "${pattern}" 2>/dev/null || true
  done | sort -u | sed '/^$/d' | wc -l | tr -d ' '
}

assert_sync_metadata() {
  local latest_count=$1
  local commit_count=$2
  local frontier_count=$3
  if [[ "${latest_count}" -le 0 ]]; then
    echo "expected latest checkpoint keys in sync mode" >&2
    exit 1
  fi
  if [[ "${commit_count}" != "0" ]]; then
    echo "expected no commit journal keys in sync mode, got ${commit_count}" >&2
    exit 1
  fi
  if [[ "${frontier_count}" != "0" ]]; then
    echo "expected no frontier keys in sync mode, got ${frontier_count}" >&2
    exit 1
  fi
}

assert_frontier_metadata() {
  local latest_count=$1
  local commit_count=$2
  local frontier_count=$3
  if [[ "${frontier_count}" -le 0 ]]; then
    echo "expected frontier keys in frontier mode" >&2
    exit 1
  fi
  if [[ "${latest_count}" != "0" ]]; then
    echo "expected no latest checkpoint keys in frontier mode, got ${latest_count}" >&2
    exit 1
  fi
  if [[ "${commit_count}" != "0" ]]; then
    echo "expected no residual commit journal keys after settled frontier replay, got ${commit_count}" >&2
    exit 1
  fi
}

wait_for_bisync_metadata() {
  local mode_arg=$1
  local replay_mode
  replay_mode=$(normalize_replay_mode "${mode_arg}")
  shift
  local ports=("$@")
  local latest_count
  local commit_count
  local frontier_count
  local metadata_counts

  for _ in $(seq 1 40); do
    latest_count=$(cluster_scan_count 'redis-gunyu-bisync:*:latest:*' "${ports[@]}")
    commit_count=$(cluster_scan_count 'redis-gunyu-bisync:*:commit:*' "${ports[@]}")
    frontier_count=$(cluster_scan_count '*:frontier' "${ports[@]}")

    if replay_mode_uses_frontier "${replay_mode}"; then
      if [[ "${frontier_count}" -gt 0 && "${latest_count}" == "0" && "${commit_count}" == "0" ]]; then
        echo "${latest_count} ${commit_count} ${frontier_count}"
        return 0
      fi
    else
      if [[ "${latest_count}" -gt 0 && "${commit_count}" == "0" && "${frontier_count}" == "0" ]]; then
        echo "${latest_count} ${commit_count} ${frontier_count}"
        return 0
      fi
    fi
    sleep 1
  done

  echo "${latest_count:-0} ${commit_count:-0} ${frontier_count:-0}"
  return 1
}

assert_resume_offsets() {
  local log_file=$1
  if ! match_regex_quiet 'reply\(\{[^ ]+ [1-9][0-9]*\}\)' "${log_file}"; then
    echo "expected non-zero psync resume offset in ${log_file}" >&2
    exit 1
  fi
}

run_scenario() {
  local mode=$1
  local mode_arg=$2
  local replay_mode
  replay_mode=$(normalize_replay_mode "${mode_arg}")
  local src_base=$3
  local dst_base=$4
  local fwd_http_port=$5
  local rev_http_port=$6
  local prefix="${TEST_PREFIX}:${mode}"
  local src_ports=("${src_base}" "$((src_base + 1))" "$((src_base + 2))")
  local dst_ports=("${dst_base}" "$((dst_base + 1))" "$((dst_base + 2))")
  local src_csv
  local dst_csv
  local latest_count
  local commit_count
  local frontier_count

  src_csv=$(printf "127.0.0.1:%s,127.0.0.1:%s,127.0.0.1:%s" "${src_ports[0]}" "${src_ports[1]}" "${src_ports[2]}")
  dst_csv=$(printf "127.0.0.1:%s,127.0.0.1:%s,127.0.0.1:%s" "${dst_ports[0]}" "${dst_ports[1]}" "${dst_ports[2]}")

  echo "[3/6] scenario ${mode}: start clusters and syncers"
  start_cluster "${mode}-src" "${src_ports[@]}"
  start_cluster "${mode}-dst" "${dst_ports[@]}"
  start_syncers "${mode}" "${src_csv}" "${dst_csv}" "${fwd_http_port}" "${rev_http_port}" "${mode_arg}"

  echo "[4/6] scenario ${mode}: phase1 writes and first convergence"
  write_phase1 "${prefix}" "${src_ports[0]}" "${dst_ports[0]}"
  wait_for_converge "${src_csv}" "${dst_csv}" "${prefix}:*"
  write_resume_seed "${prefix}" "${src_ports[0]}" "${dst_ports[0]}"
  wait_for_converge "${src_csv}" "${dst_csv}" "${prefix}:*"
  if [[ "${HALT_AFTER_PHASE1:-0}" == "1" ]]; then
    echo "halted after phase1 for inspection"
    echo "prefix=${prefix}"
    echo "forward_log=${TMP_ROOT}/${mode}-forward.log"
    echo "reverse_log=${TMP_ROOT}/${mode}-reverse.log"
    exit 0
  fi

  echo "[5/6] scenario ${mode}: stop syncers, write while offline, restart and recover"
  stop_syncers
  write_phase2 "${prefix}" "${src_ports[0]}" "${dst_ports[0]}"
  start_syncers "${mode}" "${src_csv}" "${dst_csv}" "${fwd_http_port}" "${rev_http_port}" "${mode_arg}"
  wait_for_converge "${src_csv}" "${dst_csv}" "${prefix}:*"
  "${TMP_ROOT}/bisync_compare" --left-addrs "${src_csv}" --right-addrs "${dst_csv}" --pattern "${prefix}:*"

  assert_expected_state "${src_ports[0]}" "${prefix}"
  metadata_counts=$(wait_for_bisync_metadata "${mode_arg}" "${src_ports[@]}" "${dst_ports[@]}") || {
    echo "bisync metadata did not settle for scenario ${mode}" >&2
    exit 1
  }
  read -r latest_count commit_count frontier_count <<< "${metadata_counts}"

  if replay_mode_uses_frontier "${replay_mode}"; then
    assert_frontier_metadata "${latest_count}" "${commit_count}" "${frontier_count}"
  else
    assert_sync_metadata "${latest_count}" "${commit_count}" "${frontier_count}"
  fi

  assert_resume_offsets "${TMP_ROOT}/${mode}-forward.log"
  assert_resume_offsets "${TMP_ROOT}/${mode}-reverse.log"

  echo "[6/6] scenario ${mode}: summary"
  echo "prefix=${prefix}"
  echo "mode=${replay_mode}"
  echo "latest_keys=${latest_count}"
  echo "commit_keys=${commit_count}"
  echo "frontier_keys=${frontier_count}"
  echo "forward_log=${TMP_ROOT}/${mode}-forward.log"
  echo "reverse_log=${TMP_ROOT}/${mode}-reverse.log"

  stop_syncers
  shutdown_ports "${src_ports[@]}" "${dst_ports[@]}"
}

build_binaries
run_checkpoint_namespace_tests
case ",${SCENARIOS}," in
  *",sync,"*)
    run_scenario sync sync "${SERIAL_SRC_BASE}" "${SERIAL_DST_BASE}" "${SERIAL_FWD_HTTP_PORT}" "${SERIAL_REV_HTTP_PORT}"
    ;;
esac
case ",${SCENARIOS}," in
  *",pipeline,"*)
    run_scenario pipeline pipeline "${ORDERED_SRC_BASE}" "${ORDERED_DST_BASE}" "${ORDERED_FWD_HTTP_PORT}" "${ORDERED_REV_HTTP_PORT}"
    ;;
esac
case ",${SCENARIOS}," in
  *",parallel,"*)
    run_scenario parallel parallel "${PIPELINE_SRC_BASE}" "${PIPELINE_DST_BASE}" "${PIPELINE_FWD_HTTP_PORT}" "${PIPELINE_REV_HTTP_PORT}"
    ;;
esac
