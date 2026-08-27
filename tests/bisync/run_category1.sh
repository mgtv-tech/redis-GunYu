#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_ROOT="${TMPDIR:-/tmp}/redisgunyu-bisync-cat1"
source "${ROOT}/tests/bisync/lib/redis_env.sh"
require_test_commands go redis-server redis-cli curl
SRC_PORTS=("${SRC_PORT_1:-19100}" "${SRC_PORT_2:-19101}" "${SRC_PORT_3:-19102}")
DST_PORTS=("${DST_PORT_1:-19200}" "${DST_PORT_2:-19201}" "${DST_PORT_3:-19202}")
FWD_HTTP_PORT="${FWD_HTTP_PORT:-19180}"
REV_HTTP_PORT="${REV_HTTP_PORT:-19280}"
TEST_PREFIX="${TEST_PREFIX:-bisync:cat1:$(date +%s)}"
REDIS_SERVER_BIN="$(resolve_redis_server_bin REDIS_SERVER_BIN REDIS_DEPLOY_ROOT)"

cleanup() {
  local code=$?
  set +e
  if [[ "${KEEP_SERVERS:-0}" != "1" ]]; then
    if [[ -n "${FWD_PID:-}" ]]; then
      kill "${FWD_PID}" >/dev/null 2>&1 || true
      wait "${FWD_PID}" >/dev/null 2>&1 || true
    fi
    if [[ -n "${REV_PID:-}" ]]; then
      kill "${REV_PID}" >/dev/null 2>&1 || true
      wait "${REV_PID}" >/dev/null 2>&1 || true
    fi
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
  echo "[1/8] building binaries"
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
  echo "[2/8] starting cluster ${prefix} on ports ${ports[*]}"
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

start_syncers() {
  echo "[3/8] starting bidirectional syncers"
  local src_addrs
  local dst_addrs
  src_addrs=$(printf "127.0.0.1:%s,127.0.0.1:%s,127.0.0.1:%s" "${SRC_PORTS[0]}" "${SRC_PORTS[1]}" "${SRC_PORTS[2]}")
  dst_addrs=$(printf "127.0.0.1:%s,127.0.0.1:%s,127.0.0.1:%s" "${DST_PORTS[0]}" "${DST_PORTS[1]}" "${DST_PORTS[2]}")

  write_syncer_conf "forward" "${FWD_HTTP_PORT}" "${src_addrs}" "${dst_addrs}"
  write_syncer_conf "reverse" "${REV_HTTP_PORT}" "${dst_addrs}" "${src_addrs}"

  "${TMP_ROOT}/redisGunYu" -conf "${TMP_ROOT}/forward.yaml" -cmd sync >"${TMP_ROOT}/forward.log" 2>&1 &
  FWD_PID=$!
  "${TMP_ROOT}/redisGunYu" -conf "${TMP_ROOT}/reverse.yaml" -cmd sync >"${TMP_ROOT}/reverse.log" 2>&1 &
  REV_PID=$!

  wait_for_syncer "${FWD_HTTP_PORT}"
  wait_for_syncer "${REV_HTTP_PORT}"
  sleep 2
}

redis_cmd() {
  local port=$1
  shift
  redis-cli -c -p "${port}" "$@" >/dev/null
}

write_test_data() {
  echo "[4/8] writing source-side mutations"
  redis_cmd "${SRC_PORTS[0]}" set "${TEST_PREFIX}:src:string" "alpha"
  redis_cmd "${SRC_PORTS[0]}" sadd "${TEST_PREFIX}:src:set" "red" "blue"
  redis_cmd "${SRC_PORTS[0]}" zadd "${TEST_PREFIX}:src:zset" 1 one 2 two
  redis_cmd "${SRC_PORTS[0]}" hset "${TEST_PREFIX}:src:hash" f1 v1 f2 v2
  redis_cmd "${SRC_PORTS[0]}" incrby "${TEST_PREFIX}:src:counter" 3
  redis_cmd "${SRC_PORTS[0]}" set "${TEST_PREFIX}:src:delete-me" "gone-soon"
  redis_cmd "${SRC_PORTS[0]}" del "${TEST_PREFIX}:src:delete-me"
  redis-cli -c -p "${SRC_PORTS[0]}" <<EOF >/dev/null
MULTI
INCRBY ${TEST_PREFIX}:src:txn-counter{txn-a} 5
RPUSH ${TEST_PREFIX}:src:txn-list{txn-a} a b
HSET ${TEST_PREFIX}:src:txn-hash{txn-a} field value
EXEC
EOF

  echo "[5/8] writing destination-side mutations"
  redis_cmd "${DST_PORTS[0]}" set "${TEST_PREFIX}:dst:string" "beta"
  redis_cmd "${DST_PORTS[0]}" sadd "${TEST_PREFIX}:dst:set" "left" "right"
  redis_cmd "${DST_PORTS[0]}" zadd "${TEST_PREFIX}:dst:zset" 7 seven 9 nine
  redis_cmd "${DST_PORTS[0]}" hset "${TEST_PREFIX}:dst:hash" g1 w1 g2 w2
  redis_cmd "${DST_PORTS[0]}" incrby "${TEST_PREFIX}:dst:counter" 11
  redis_cmd "${DST_PORTS[0]}" lpush "${TEST_PREFIX}:dst:list" x y z
  redis_cmd "${DST_PORTS[0]}" set "${TEST_PREFIX}:dst:delete-me" "gone-soon"
  redis_cmd "${DST_PORTS[0]}" del "${TEST_PREFIX}:dst:delete-me"
  redis-cli -c -p "${DST_PORTS[0]}" <<EOF >/dev/null
MULTI
INCRBY ${TEST_PREFIX}:dst:txn-counter{txn-b} 4
LPUSH ${TEST_PREFIX}:dst:txn-list{txn-b} q r
HINCRBY ${TEST_PREFIX}:dst:txn-hash{txn-b} hits 2
EXEC
EOF
}

compare_clusters() {
  "${TMP_ROOT}/bisync_compare" \
    -left-addrs "127.0.0.1:${SRC_PORTS[0]},127.0.0.1:${SRC_PORTS[1]},127.0.0.1:${SRC_PORTS[2]}" \
    -right-addrs "127.0.0.1:${DST_PORTS[0]},127.0.0.1:${DST_PORTS[1]},127.0.0.1:${DST_PORTS[2]}" \
    -pattern "${TEST_PREFIX}:*"
}

wait_for_consistency() {
  echo "[6/8] waiting for both clusters to converge"
  for _ in $(seq 1 80); do
    if compare_clusters >/dev/null 2>&1; then
      sleep 3
      compare_clusters
      return 0
    fi
    sleep 0.5
  done
  echo "clusters did not converge in time" >&2
  compare_clusters
}

dump_status() {
  echo "[7/8] syncer status"
  curl -sf "http://127.0.0.1:${FWD_HTTP_PORT}/syncer/status" || true
  printf "\n"
  curl -sf "http://127.0.0.1:${REV_HTTP_PORT}/syncer/status" || true
  printf "\n"
}

print_summary() {
  echo "[8/8] category1 summary"
  echo "prefix=${TEST_PREFIX}"
  echo "source_cluster=127.0.0.1:${SRC_PORTS[0]},127.0.0.1:${SRC_PORTS[1]},127.0.0.1:${SRC_PORTS[2]}"
  echo "target_cluster=127.0.0.1:${DST_PORTS[0]},127.0.0.1:${DST_PORTS[1]},127.0.0.1:${DST_PORTS[2]}"
  echo "forward_log=${TMP_ROOT}/forward.log"
  echo "reverse_log=${TMP_ROOT}/reverse.log"
}

build_binaries
start_cluster "src" "${SRC_PORTS[@]}"
start_cluster "dst" "${DST_PORTS[@]}"
start_syncers
write_test_data
wait_for_consistency
dump_status
print_summary
