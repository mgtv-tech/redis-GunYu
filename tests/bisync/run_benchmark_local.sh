#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/bisync/lib/redis_env.sh"
require_test_commands go redis-server redis-cli curl python3

BENCH_TMP_ROOT="${TMP_ROOT:-${TMPDIR:-/tmp}/redisgunyu-bisync-benchmark-local}"
CLUSTER_TMP_ROOT="${CLUSTER_TMP_ROOT:-${TMPDIR:-/tmp}/redisgunyu-bisync-benchmark-local-cluster}"
LEFT_PORTS=("${LEFT_PORT_1:-33700}" "${LEFT_PORT_2:-33701}" "${LEFT_PORT_3:-33702}")
RIGHT_PORTS=("${RIGHT_PORT_1:-33800}" "${RIGHT_PORT_2:-33801}" "${RIGHT_PORT_3:-33802}")
REDIS_SERVER_BIN="$(resolve_redis_server_bin REDIS_SERVER_BIN REDIS_DEPLOY_ROOT)"

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
  for _ in $(seq 1 80); do
    if redis-cli -p "${port}" ping >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  echo "redis on port ${port} did not start" >&2
  return 1
}

wait_for_cluster_ok() {
  local port=$1
  for _ in $(seq 1 160); do
    if redis-cli -p "${port}" cluster info 2>/dev/null | match_regex_quiet '^cluster_state:ok'; then
      return 0
    fi
    sleep 0.25
  done
  echo "cluster on port ${port} did not become ready" >&2
  return 1
}

start_cluster() {
  local prefix=$1
  shift
  local ports=("$@")
  local port

  echo "starting cluster ${prefix} on ports ${ports[*]}"
  for port in "${ports[@]}"; do
    write_redis_conf "${CLUSTER_TMP_ROOT}/${prefix}-${port}" "${port}"
    "${REDIS_SERVER_BIN}" "${CLUSTER_TMP_ROOT}/${prefix}-${port}/redis.conf"
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
}

shutdown_ports() {
  local port
  for port in "$@"; do
    redis-cli -p "${port}" shutdown nosave >/dev/null 2>&1 || true
  done
}

cleanup() {
  local code=$?
  set +e
  if [[ "${KEEP_SERVERS:-0}" != "1" ]]; then
    shutdown_ports "${LEFT_PORTS[@]}" "${RIGHT_PORTS[@]}"
  fi
  if [[ "${KEEP_TMP:-0}" != "1" ]]; then
    rm -rf "${CLUSTER_TMP_ROOT}"
  fi
  exit "${code}"
}
trap cleanup EXIT

rm -rf "${CLUSTER_TMP_ROOT}"
mkdir -p "${CLUSTER_TMP_ROOT}"

start_cluster left "${LEFT_PORTS[@]}"
start_cluster right "${RIGHT_PORTS[@]}"

LEFT_ADDRS="$(printf '127.0.0.1:%s,127.0.0.1:%s,127.0.0.1:%s' "${LEFT_PORTS[0]}" "${LEFT_PORTS[1]}" "${LEFT_PORTS[2]}")"
RIGHT_ADDRS="$(printf '127.0.0.1:%s,127.0.0.1:%s,127.0.0.1:%s' "${RIGHT_PORTS[0]}" "${RIGHT_PORTS[1]}" "${RIGHT_PORTS[2]}")"

TMP_ROOT="${BENCH_TMP_ROOT}"
ALLOW_DESTRUCTIVE_REDIS_TESTS=1
TEST_ENVIRONMENT_ID="local-benchmark-$$"
export LEFT_ADDRS RIGHT_ADDRS TMP_ROOT ALLOW_DESTRUCTIVE_REDIS_TESTS TEST_ENVIRONMENT_ID
"${ROOT}/tests/bisync/run_benchmark.sh"
