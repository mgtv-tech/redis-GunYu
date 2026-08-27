#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_ROOT="${TMPDIR:-/tmp}/redisgunyu-bisync-cat8"
source "${ROOT}/tests/bisync/lib/redis_env.sh"
require_test_commands go redis-server redis-cli curl
SRC_PORTS=("${SRC_PORT_1:-30700}" "${SRC_PORT_2:-30701}" "${SRC_PORT_3:-30702}")
DST_PORTS=("${DST_PORT_1:-30800}" "${DST_PORT_2:-30801}" "${DST_PORT_3:-30802}")
HTTP_PORT="${HTTP_PORT:-30780}"
TEST_PREFIX="${TEST_PREFIX:-bisync:cat8:$(date +%s)}"
SYNCER_PID=""
REDIS_SERVER_BIN="$(resolve_redis_server_bin REDIS_SERVER_BIN REDIS_DEPLOY_ROOT)"

cleanup() {
  local code=$?
  set +e
  if [[ -n "${SYNCER_PID}" ]]; then
    kill "${SYNCER_PID}" >/dev/null 2>&1 || true
    wait "${SYNCER_PID}" >/dev/null 2>&1 || true
    SYNCER_PID=""
  fi
  if [[ "${KEEP_SERVERS:-0}" != "1" ]]; then
    for p in "${SRC_PORTS[@]}" "${DST_PORTS[@]}"; do
      redis-cli -p "${p}" shutdown nosave >/dev/null 2>&1 || true
    done
  fi
  if [[ "${KEEP_TMP:-0}" != "1" ]]; then
    rm -rf "${TMP_ROOT}"
  fi
  exit "${code}"
}
trap cleanup EXIT

rm -rf "${TMP_ROOT}"
mkdir -p "${TMP_ROOT}"

build_binaries() {
  echo "[1/7] building binaries"
  mkdir -p "${TMP_ROOT}/gocache"
  (cd "${ROOT}" && GOCACHE="${TMP_ROOT}/gocache" go build -o "${TMP_ROOT}/redisGunYu" ./main.go)
  (cd "${ROOT}" && GOCACHE="${TMP_ROOT}/gocache" go build -o "${TMP_ROOT}/bisync_compare" ./tests/bisync/cmd/bisync_compare)
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
  echo "[2/7] starting cluster ${prefix} on ports ${ports[*]}"
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
    bisyncEnabled: false
    mode: sync
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
  local src_addrs=$1
  local dst_addrs=$2
  echo "[3/7] starting single-direction non-bisync syncer"
  write_syncer_conf "forward" "${HTTP_PORT}" "${src_addrs}" "${dst_addrs}"
  "${TMP_ROOT}/redisGunYu" -conf "${TMP_ROOT}/forward.yaml" -cmd sync >"${TMP_ROOT}/forward.log" 2>&1 &
  SYNCER_PID=$!
  wait_for_syncer "${HTTP_PORT}"
  sleep 2
}

redis_cmd() {
  local port=$1
  shift
  redis-cli -c -p "${port}" "$@" >/dev/null
}

write_source_data() {
  echo "[4/7] writing source-side mutations"
  redis_cmd "${SRC_PORTS[0]}" set "${TEST_PREFIX}:string" "alpha"
  redis_cmd "${SRC_PORTS[0]}" incrby "${TEST_PREFIX}:counter" 9
  redis_cmd "${SRC_PORTS[0]}" sadd "${TEST_PREFIX}:set" red blue
  redis_cmd "${SRC_PORTS[0]}" hset "${TEST_PREFIX}:hash" f1 v1 f2 v2
  redis_cmd "${SRC_PORTS[0]}" rpush "${TEST_PREFIX}:list" a b c
  redis_cmd "${SRC_PORTS[0]}" zadd "${TEST_PREFIX}:zset" 1 one 2 two
  redis_cmd "${SRC_PORTS[0]}" set "${TEST_PREFIX}:delete-me" "gone"
  redis_cmd "${SRC_PORTS[0]}" del "${TEST_PREFIX}:delete-me"
}

compare_clusters() {
  "${TMP_ROOT}/bisync_compare" \
    --left-addrs "127.0.0.1:${SRC_PORTS[0]},127.0.0.1:${SRC_PORTS[1]},127.0.0.1:${SRC_PORTS[2]}" \
    --right-addrs "127.0.0.1:${DST_PORTS[0]},127.0.0.1:${DST_PORTS[1]},127.0.0.1:${DST_PORTS[2]}" \
    --pattern "${TEST_PREFIX}:*"
}

wait_for_consistency() {
  echo "[5/7] waiting for cluster convergence"
  for _ in $(seq 1 80); do
    if compare_clusters >/dev/null 2>&1; then
      sleep 2
      compare_clusters
      return 0
    fi
    sleep 0.5
  done
  compare_clusters
}

cluster_scan_count() {
  local pattern=$1
  shift
  local port
  for port in "$@"; do
    redis-cli -p "${port}" --scan --pattern "${pattern}" 2>/dev/null || true
  done | sort -u | sed '/^$/d' | wc -l | tr -d ' '
}

assert_no_bisync_metadata() {
  local marker_count latest_count commit_count frontier_count
  marker_count=$(cluster_scan_count 'redis-gunyu-bisync:*:marker:*' "${DST_PORTS[@]}")
  latest_count=$(cluster_scan_count 'redis-gunyu-bisync:*:latest:*' "${DST_PORTS[@]}")
  commit_count=$(cluster_scan_count 'redis-gunyu-bisync:*:commit:*' "${DST_PORTS[@]}")
  frontier_count=$(cluster_scan_count '*:frontier' "${DST_PORTS[@]}")
  if [[ "${marker_count}" != "0" || "${latest_count}" != "0" || "${commit_count}" != "0" || "${frontier_count}" != "0" ]]; then
    echo "expected non-bisync replay to leave no bisync metadata, got marker=${marker_count} latest=${latest_count} commit=${commit_count} frontier=${frontier_count}" >&2
    exit 1
  fi
}

assert_log_is_non_bisync() {
  if match_regex_quiet 'bisync startpoint|bisync checkpoint namespace|scheme1|frontier' "${TMP_ROOT}/forward.log"; then
    echo "expected non-bisync run to avoid bisync recovery path log markers" >&2
    exit 1
  fi
}

print_summary() {
  echo "[6/7] syncer status"
  curl -sf "http://127.0.0.1:${HTTP_PORT}/syncer/status" || true
  printf "\n"
  echo "[7/7] category8 summary"
  echo "prefix=${TEST_PREFIX}"
  echo "forward_log=${TMP_ROOT}/forward.log"
}

build_binaries
start_cluster "src" "${SRC_PORTS[@]}"
start_cluster "dst" "${DST_PORTS[@]}"
src_addrs=$(printf "127.0.0.1:%s,127.0.0.1:%s,127.0.0.1:%s" "${SRC_PORTS[0]}" "${SRC_PORTS[1]}" "${SRC_PORTS[2]}")
dst_addrs=$(printf "127.0.0.1:%s,127.0.0.1:%s,127.0.0.1:%s" "${DST_PORTS[0]}" "${DST_PORTS[1]}" "${DST_PORTS[2]}")
start_syncer "${src_addrs}" "${dst_addrs}"
write_source_data
wait_for_consistency
assert_no_bisync_metadata
assert_log_is_non_bisync
print_summary
