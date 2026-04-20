#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/bisync/lib/redis_env.sh"

BENCH_TMP_ROOT="${TMP_ROOT:-${TMPDIR:-/tmp}/redisgunyu-bisync-benchmark-cloud-local}"
CLUSTER_TMP_ROOT="${CLUSTER_TMP_ROOT:-${TMPDIR:-/tmp}/redisgunyu-bisync-benchmark-cloud-local-cluster}"
PROXY_TMP_ROOT="${PROXY_TMP_ROOT:-${TMPDIR:-/tmp}/redisgunyu-bisync-benchmark-cloud-local-proxy}"

LEFT_PORTS=("${LEFT_PORT_1:-33700}" "${LEFT_PORT_2:-33701}" "${LEFT_PORT_3:-33702}")
RIGHT_PORTS=("${RIGHT_PORT_1:-33800}" "${RIGHT_PORT_2:-33801}" "${RIGHT_PORT_3:-33802}")

LEFT_PROXY_PORTS=("${LEFT_PROXY_PORT_1:-34700}" "${LEFT_PROXY_PORT_2:-34701}" "${LEFT_PROXY_PORT_3:-34702}")
RIGHT_PROXY_PORTS=("${RIGHT_PROXY_PORT_1:-34800}" "${RIGHT_PROXY_PORT_2:-34801}" "${RIGHT_PROXY_PORT_3:-34802}")

WAN_LATENCY="${WAN_LATENCY:-40ms}"
WAN_JITTER="${WAN_JITTER:-10ms}"
WAN_PROXY_SEED="${WAN_PROXY_SEED:-20260418}"

REDIS_SERVER_BIN="$(resolve_redis_server_bin REDIS_SERVER_BIN REDIS_DEPLOY_ROOT)"
NETEM_PROXY_BIN="${NETEM_PROXY_BIN:-${BENCH_TMP_ROOT}/redis_netem_proxy}"

PROXY_PIDS=()

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
    if redis-cli -p "${port}" cluster info 2>/dev/null | rg -q '^cluster_state:ok'; then
      return 0
    fi
    sleep 0.25
  done
  echo "cluster on port ${port} did not become ready" >&2
  return 1
}

wait_for_listen() {
  local port=$1
  for _ in $(seq 1 80); do
    if nc -z 127.0.0.1 "${port}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  echo "proxy on port ${port} did not start" >&2
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

stop_proxies() {
  local pid
  for pid in "${PROXY_PIDS[@]:-}"; do
    kill "${pid}" >/dev/null 2>&1 || true
    wait "${pid}" >/dev/null 2>&1 || true
  done
  PROXY_PIDS=()
}

cleanup() {
  local code=$?
  set +e
  stop_proxies
  if [[ "${KEEP_SERVERS:-0}" != "1" ]]; then
    shutdown_ports "${LEFT_PORTS[@]}" "${RIGHT_PORTS[@]}"
  fi
  if [[ "${KEEP_TMP:-0}" != "1" ]]; then
    rm -rf "${CLUSTER_TMP_ROOT}" "${PROXY_TMP_ROOT}"
  fi
  exit "${code}"
}
trap cleanup EXIT

build_proxy() {
  mkdir -p "${BENCH_TMP_ROOT}/gocache"
  (cd "${ROOT}" && GOCACHE="${BENCH_TMP_ROOT}/gocache" go build -o "${NETEM_PROXY_BIN}" ./tests/bisync/cmd/redis_netem_proxy)
}

join_addr_csv() {
  local ports_name=$1
  eval "local ports_ref=(\"\${${ports_name}[@]}\")"
  local out=()
  local port
  for port in "${ports_ref[@]}"; do
    out+=("127.0.0.1:${port}")
  done
  printf '%s\n' "$(IFS=,; echo "${out[*]}")"
}

rewrite_csv() {
  local upstream_name=$1
  local proxy_name=$2
  eval "local upstream_ref=(\"\${${upstream_name}[@]}\")"
  eval "local proxy_ref=(\"\${${proxy_name}[@]}\")"
  local pairs=()
  local idx
  for idx in "${!upstream_ref[@]}"; do
    pairs+=("127.0.0.1:${upstream_ref[$idx]}=127.0.0.1:${proxy_ref[$idx]}")
  done
  printf '%s\n' "$(IFS=,; echo "${pairs[*]}")"
}

start_proxy_group() {
  local prefix=$1
  local latency=$2
  local jitter=$3
  local rewrite=$4
  local listen_name=$5
  local upstream_name=$6
  eval "local listen_ref=(\"\${${listen_name}[@]}\")"
  eval "local upstream_ref=(\"\${${upstream_name}[@]}\")"
  local idx

  mkdir -p "${PROXY_TMP_ROOT}"
  for idx in "${!listen_ref[@]}"; do
    "${NETEM_PROXY_BIN}" \
      --listen "127.0.0.1:${listen_ref[$idx]}" \
      --upstream "127.0.0.1:${upstream_ref[$idx]}" \
      --latency "${latency}" \
      --jitter "${jitter}" \
      --seed "$((WAN_PROXY_SEED + idx))" \
      --rewrite-map "${rewrite}" \
      > "${PROXY_TMP_ROOT}/${prefix}-${listen_ref[$idx]}.log" 2>&1 &
    PROXY_PIDS+=("$!")
    wait_for_listen "${listen_ref[$idx]}"
  done
}

rm -rf "${CLUSTER_TMP_ROOT}" "${PROXY_TMP_ROOT}"
mkdir -p "${CLUSTER_TMP_ROOT}" "${PROXY_TMP_ROOT}" "${BENCH_TMP_ROOT}"

start_cluster cloud-a "${LEFT_PORTS[@]}"
start_cluster cloud-b "${RIGHT_PORTS[@]}"
build_proxy

LEFT_ADDRS="$(join_addr_csv LEFT_PORTS)"
RIGHT_ADDRS="$(join_addr_csv RIGHT_PORTS)"
LEFT_PROXY_ADDRS="$(join_addr_csv LEFT_PROXY_PORTS)"
RIGHT_PROXY_ADDRS="$(join_addr_csv RIGHT_PROXY_PORTS)"

LEFT_REWRITE="$(rewrite_csv LEFT_PORTS LEFT_PROXY_PORTS)"
RIGHT_REWRITE="$(rewrite_csv RIGHT_PORTS RIGHT_PROXY_PORTS)"

start_proxy_group "cloud-b-to-cloud-a" "${WAN_LATENCY}" "${WAN_JITTER}" "${LEFT_REWRITE}" LEFT_PROXY_PORTS LEFT_PORTS
start_proxy_group "cloud-a-to-cloud-b" "${WAN_LATENCY}" "${WAN_JITTER}" "${RIGHT_REWRITE}" RIGHT_PROXY_PORTS RIGHT_PORTS

TMP_ROOT="${BENCH_TMP_ROOT}"
FWD_INPUT_ADDRS="${LEFT_ADDRS}"
FWD_OUTPUT_ADDRS="${RIGHT_PROXY_ADDRS}"
REV_INPUT_ADDRS="${RIGHT_ADDRS}"
REV_OUTPUT_ADDRS="${LEFT_PROXY_ADDRS}"
NETWORK_PROFILE="cross-cloud redis netem proxy latency=${WAN_LATENCY} jitter=${WAN_JITTER}; direct local workload path; cloud-a=${LEFT_ADDRS}; cloud-b=${RIGHT_ADDRS}"

export TMP_ROOT LEFT_ADDRS RIGHT_ADDRS FWD_INPUT_ADDRS FWD_OUTPUT_ADDRS REV_INPUT_ADDRS REV_OUTPUT_ADDRS NETWORK_PROFILE
"${ROOT}/tests/bisync/run_benchmark.sh"
