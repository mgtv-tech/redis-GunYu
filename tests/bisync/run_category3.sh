#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_ROOT="${TMPDIR:-/tmp}/redisgunyu-bisync-cat3"
source "${ROOT}/tests/bisync/lib/redis_env.sh"
TEST_PREFIX="${TEST_PREFIX:-bisync:cat3:$(date +%s)}"
SCENARIOS="${SCENARIOS:-serial,pipeline}"
SERIAL_SRC_BASE="${SERIAL_SRC_BASE:-29700}"
SERIAL_DST_BASE="${SERIAL_DST_BASE:-29800}"
SERIAL_HTTP_PORT="${SERIAL_HTTP_PORT:-29780}"
PIPELINE_SRC_BASE="${PIPELINE_SRC_BASE:-29900}"
PIPELINE_DST_BASE="${PIPELINE_DST_BASE:-30000}"
PIPELINE_HTTP_PORT="${PIPELINE_HTTP_PORT:-29980}"
SYNCER_PID=""
REDIS_SERVER_BIN="$(resolve_redis_server_bin REDIS_SERVER_BIN REDIS_DEPLOY_ROOT)"

cleanup() {
  local code=$?
  set +e
  stop_syncer
  shutdown_ports $(category3_all_ports)
  if [[ "${KEEP_TMP:-0}" != "1" ]]; then
    rm -rf "${TMP_ROOT}"
  fi
  exit "${code}"
}
trap cleanup EXIT

stop_syncer() {
  if [[ -n "${SYNCER_PID}" ]]; then
    kill "${SYNCER_PID}" >/dev/null 2>&1 || true
    wait "${SYNCER_PID}" >/dev/null 2>&1 || true
    SYNCER_PID=""
  fi
}

shutdown_ports() {
  local port
  for port in "$@"; do
    redis-cli -p "${port}" shutdown nosave >/dev/null 2>&1 || true
  done
}

category3_all_ports() {
  printf '%s\n' \
    "${SERIAL_SRC_BASE}" "$((SERIAL_SRC_BASE + 1))" "$((SERIAL_SRC_BASE + 2))" \
    "${SERIAL_DST_BASE}" "$((SERIAL_DST_BASE + 1))" "$((SERIAL_DST_BASE + 2))" \
    "${PIPELINE_SRC_BASE}" "$((PIPELINE_SRC_BASE + 1))" "$((PIPELINE_SRC_BASE + 2))" \
    "${PIPELINE_DST_BASE}" "$((PIPELINE_DST_BASE + 1))" "$((PIPELINE_DST_BASE + 2))"
}

rm -rf "${TMP_ROOT}"
mkdir -p "${TMP_ROOT}"

build_binaries() {
  echo "[1/5] building binaries"
  mkdir -p "${TMP_ROOT}/gocache"
  (cd "${ROOT}" && GOCACHE="${TMP_ROOT}/gocache" go build -o "${TMP_ROOT}/redisGunYu" ./main.go)
  (cd "${ROOT}" && GOCACHE="${TMP_ROOT}/gocache" go build -o "${TMP_ROOT}/bisync_compare" ./tests/bisync/cmd/bisync_compare)
}

run_rdb_unit_tests() {
  echo "[2/5] running focused RDB unit tests"
  (cd "${ROOT}" && GOCACHE="${TMP_ROOT}/gocache" go test ./syncer -run 'TestParseAofReplayUnitsSuppressesMirroredRdbTxn|TestParseAofReplayUnitsSuppressesMirroredRdbTxnWithPexpireatRewrite|TestBuildBisyncRdbReplayUnitReplacePrependsDeleteAndRewritesKey|TestBuildBisyncRdbReplayUnitReplaceUsesRestoreWhenSupported|TestBuildBisyncRdbReplayUnitIgnoreSkipsSplitKeyOnce|TestBuildBisyncRdbReplayUnitIgnoreDoesNotLeakSkippedKeyAcrossEntries|TestBuildBisyncRdbReplayUnitErrorFailsIfKeyExists|TestBisyncRdbIsGlobalEntryForClusterFunctions|TestExecBisyncRdbUnitWritesMarkerAndBusinessCommands' -count=1)
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
    if redis-cli -p "${port}" cluster info 2>/dev/null | rg -q '^cluster_state:ok'; then
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
  echo "[3/5] starting cluster ${prefix} on ports ${ports[*]}"
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
  local pipeline_flag=$5
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
    enableAofPipeline: ${pipeline_flag}
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

start_syncer() {
  local name=$1
  local src_ports_csv=$2
  local dst_ports_csv=$3
  local http_port=$4
  local pipeline_flag=$5

  write_syncer_conf "${name}" "${http_port}" "${src_ports_csv}" "${dst_ports_csv}" "${pipeline_flag}"
  "${TMP_ROOT}/redisGunYu" -conf "${TMP_ROOT}/${name}.yaml" -cmd sync > "${TMP_ROOT}/${name}.log" 2>&1 &
  SYNCER_PID=$!
  wait_for_syncer "${http_port}"
}

redis_cmd() {
  local port=$1
  shift
  redis-cli -c -p "${port}" "$@" >/dev/null
}

write_source_fullsync_data() {
  local prefix=$1
  local port=$2
  redis_cmd "${port}" set "${prefix}:string{cat3-a}" "alpha"
  redis_cmd "${port}" hset "${prefix}:hash{cat3-b}" f1 v1 f2 v2
  redis_cmd "${port}" sadd "${prefix}:set{cat3-c}" red blue
  redis_cmd "${port}" rpush "${prefix}:list{cat3-d}" x y z
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

cluster_scan_count() {
  local pattern=$1
  shift
  local port
  for port in "$@"; do
    redis-cli -p "${port}" --scan --pattern "${pattern}" 2>/dev/null || true
  done | sort -u | sed '/^$/d' | wc -l | tr -d ' '
}

wait_for_rdb_metadata_shape() {
  shift
  local ports=("$@")
  local marker_count
  local rdb_count
  local latest_count
  local commit_count
  local frontier_count

  for _ in $(seq 1 40); do
    marker_count=$(cluster_scan_count 'redis-gunyu-bisync:*:marker:*' "${ports[@]}")
    rdb_count=$(cluster_scan_count 'redis-gunyu-bisync:*:rdb:*' "${ports[@]}")
    latest_count=$(cluster_scan_count 'redis-gunyu-bisync:*:latest:*' "${ports[@]}")
    commit_count=$(cluster_scan_count 'redis-gunyu-bisync:*:commit:*' "${ports[@]}")
    frontier_count=$(cluster_scan_count '*:frontier' "${ports[@]}")

    if [[ "${marker_count}" -gt 0 && "${latest_count}" == "0" && "${commit_count}" == "0" && "${frontier_count}" == "0" ]]; then
      echo "${marker_count} ${rdb_count} ${latest_count} ${commit_count} ${frontier_count}"
      return 0
    fi
    sleep 1
  done

  echo "${marker_count:-0} ${rdb_count:-0} ${latest_count:-0} ${commit_count:-0} ${frontier_count:-0}"
  return 1
}

assert_expected_fullsync_state() {
  local port=$1
  local prefix=$2
  expect_eq "$(redis-cli -c -p "${port}" --raw get "${prefix}:string{cat3-a}")" "alpha" "${prefix}:string{cat3-a}"
  expect_eq "$(hash_state "${port}" "${prefix}:hash{cat3-b}")" "f1=v1|f2=v2" "${prefix}:hash{cat3-b}"
  expect_eq "$(set_state "${port}" "${prefix}:set{cat3-c}")" "blue red" "${prefix}:set{cat3-c}"
  expect_eq "$(list_state "${port}" "${prefix}:list{cat3-d}")" "x y z" "${prefix}:list{cat3-d}"
}

run_fullsync_barrier_scenario() {
  local mode=$1
  local pipeline_flag=$2
  local prefix="${TEST_PREFIX}:${mode}"
  local src_ports
  local dst_ports
  local http_port
  local src_csv
  local dst_csv
  local metadata_counts
  local marker_count
  local rdb_count
  local latest_count
  local commit_count
  local frontier_count

  if [[ "${mode}" == "serial" ]]; then
    src_ports=("${SERIAL_SRC_BASE}" "$((SERIAL_SRC_BASE + 1))" "$((SERIAL_SRC_BASE + 2))")
    dst_ports=("${SERIAL_DST_BASE}" "$((SERIAL_DST_BASE + 1))" "$((SERIAL_DST_BASE + 2))")
    http_port="${SERIAL_HTTP_PORT}"
  else
    src_ports=("${PIPELINE_SRC_BASE}" "$((PIPELINE_SRC_BASE + 1))" "$((PIPELINE_SRC_BASE + 2))")
    dst_ports=("${PIPELINE_DST_BASE}" "$((PIPELINE_DST_BASE + 1))" "$((PIPELINE_DST_BASE + 2))")
    http_port="${PIPELINE_HTTP_PORT}"
  fi

  src_csv=$(printf "127.0.0.1:%s,127.0.0.1:%s,127.0.0.1:%s" "${src_ports[0]}" "${src_ports[1]}" "${src_ports[2]}")
  dst_csv=$(printf "127.0.0.1:%s,127.0.0.1:%s,127.0.0.1:%s" "${dst_ports[0]}" "${dst_ports[1]}" "${dst_ports[2]}")

  start_cluster "${mode}-src" "${src_ports[@]}"
  start_cluster "${mode}-dst" "${dst_ports[@]}"

  echo "[4/5] scenario ${mode}: preload source and run pure full sync"
  write_source_fullsync_data "${prefix}" "${src_ports[0]}"
  start_syncer "${mode}" "${src_csv}" "${dst_csv}" "${http_port}" "${pipeline_flag}"
  wait_for_converge "${src_csv}" "${dst_csv}" "${prefix}:*"
  "${TMP_ROOT}/bisync_compare" --left-addrs "${src_csv}" --right-addrs "${dst_csv}" --pattern "${prefix}:*"
  assert_expected_fullsync_state "${dst_ports[0]}" "${prefix}"

  metadata_counts=$(wait_for_rdb_metadata_shape "${pipeline_flag}" "${dst_ports[@]}") || {
    echo "rdb metadata shape did not settle for scenario ${mode}" >&2
    exit 1
  }
  read -r marker_count rdb_count latest_count commit_count frontier_count <<< "${metadata_counts}"

  echo "[5/5] scenario ${mode}: summary"
  echo "prefix=${prefix}"
  echo "pipeline=${pipeline_flag}"
  echo "marker_keys=${marker_count}"
  echo "rdb_keys=${rdb_count}"
  echo "latest_keys=${latest_count}"
  echo "commit_keys=${commit_count}"
  echo "frontier_keys=${frontier_count}"
  echo "syncer_log=${TMP_ROOT}/${mode}.log"

  stop_syncer
  shutdown_ports "${src_ports[@]}" "${dst_ports[@]}"
}

build_binaries
run_rdb_unit_tests

case ",${SCENARIOS}," in
  *",serial,"*)
    run_fullsync_barrier_scenario serial false
    ;;
esac
case ",${SCENARIOS}," in
  *",pipeline,"*)
    run_fullsync_barrier_scenario pipeline true
    ;;
esac
