#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/bisync/lib/redis_env.sh"

SOAK_TIER="${SOAK_TIER:-2h}"
case "${SOAK_TIER}" in
  2|2h)
    SOAK_TIER="2h"
    NEXT_TIER="4h"
    ;;
  4|4h)
    SOAK_TIER="4h"
    NEXT_TIER="6h"
    ;;
  6|6h)
    SOAK_TIER="6h"
    NEXT_TIER=""
    ;;
  *)
    echo "unsupported SOAK_TIER=${SOAK_TIER}; expected 2h, 4h, or 6h" >&2
    exit 2
    ;;
esac

SOAK_DURATION="${SOAK_DURATION:-${SOAK_TIER}}"
TMP_ROOT="${TMPDIR:-/tmp}/redisgunyu-bisync-cat9-${SOAK_TIER}"
TEST_PREFIX="${TEST_PREFIX:-bisync:cat9:$(date +%s)}"
SCENARIOS="${SCENARIOS:-sync,pipeline,parallel}"
SOAK_KEY_SPACE="${SOAK_KEY_SPACE:-10000}"
SOAK_THROTTLE="${SOAK_THROTTLE:-0ms}"
SOAK_TARGET_QPS="${SOAK_TARGET_QPS:-10000}"
SOAK_WORKERS="${SOAK_WORKERS:-4}"
SOAK_BOUNDARY_EVERY="${SOAK_BOUNDARY_EVERY:-5000}"
SOAK_VOLATILE_EVERY="${SOAK_VOLATILE_EVERY:-1000}"
SOAK_TXN_EVERY="${SOAK_TXN_EVERY:-500}"
SOAK_FINAL_SETTLE_SECONDS="${SOAK_FINAL_SETTLE_SECONDS:-60}"
SOAK_OFFLINE_SECONDS="${SOAK_OFFLINE_SECONDS:-180}"
MONITOR_INTERVAL_SECONDS="${MONITOR_INTERVAL_SECONDS:-30}"
FAIL_ON_RESOURCE_WARNING="${FAIL_ON_RESOURCE_WARNING:-0}"
MAX_RSS_GROWTH_PERCENT="${MAX_RSS_GROWTH_PERCENT:-80}"
MAX_GOROUTINE_GROWTH_PERCENT="${MAX_GOROUTINE_GROWTH_PERCENT:-100}"
ALLOW_UNSUPPORTED_REDIS="${ALLOW_UNSUPPORTED_REDIS:-0}"

SERIAL_SRC_BASE="${SERIAL_SRC_BASE:-32300}"
SERIAL_DST_BASE="${SERIAL_DST_BASE:-32400}"
SERIAL_HTTP_PORT="${SERIAL_HTTP_PORT:-32380}"
SERIAL_REV_HTTP_PORT="${SERIAL_REV_HTTP_PORT:-32480}"
PIPELINE_SRC_BASE="${PIPELINE_SRC_BASE:-32500}"
PIPELINE_DST_BASE="${PIPELINE_DST_BASE:-32600}"
PIPELINE_FWD_HTTP_PORT="${PIPELINE_FWD_HTTP_PORT:-32580}"
PIPELINE_REV_HTTP_PORT="${PIPELINE_REV_HTTP_PORT:-32680}"

FWD_PID=""
REV_PID=""
MONITOR_PID=""
WORKLOAD_PID=""
REDIS_SERVER_BIN="$(resolve_redis_server_bin REDIS_SERVER_BIN REDIS_DEPLOY_ROOT)"

duration_to_seconds() {
  local duration=$1
  case "${duration}" in
    *h) echo $((${duration%h} * 3600)) ;;
    *m) echo $((${duration%m} * 60)) ;;
    *s) echo "${duration%s}" ;;
    *) echo "${duration}" ;;
  esac
}

join_csv() {
  local IFS=,
  echo "$*"
}

all_ports() {
  printf '%s\n' \
    "${SERIAL_SRC_BASE}" "$((SERIAL_SRC_BASE + 1))" "$((SERIAL_SRC_BASE + 2))" "$((SERIAL_SRC_BASE + 3))" "$((SERIAL_SRC_BASE + 4))" "$((SERIAL_SRC_BASE + 5))" \
    "${SERIAL_DST_BASE}" "$((SERIAL_DST_BASE + 1))" "$((SERIAL_DST_BASE + 2))" "$((SERIAL_DST_BASE + 3))" "$((SERIAL_DST_BASE + 4))" "$((SERIAL_DST_BASE + 5))" \
    "${PIPELINE_SRC_BASE}" "$((PIPELINE_SRC_BASE + 1))" "$((PIPELINE_SRC_BASE + 2))" "$((PIPELINE_SRC_BASE + 3))" "$((PIPELINE_SRC_BASE + 4))" "$((PIPELINE_SRC_BASE + 5))" \
    "${PIPELINE_DST_BASE}" "$((PIPELINE_DST_BASE + 1))" "$((PIPELINE_DST_BASE + 2))" "$((PIPELINE_DST_BASE + 3))" "$((PIPELINE_DST_BASE + 4))" "$((PIPELINE_DST_BASE + 5))"
}

cleanup() {
  local code=$?
  set +e
  if [[ -n "${MONITOR_PID}" ]]; then
    kill "${MONITOR_PID}" >/dev/null 2>&1 || true
    wait "${MONITOR_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${WORKLOAD_PID}" ]]; then
    kill "${WORKLOAD_PID}" >/dev/null 2>&1 || true
    wait "${WORKLOAD_PID}" >/dev/null 2>&1 || true
  fi
  stop_syncers
  shutdown_ports $(all_ports)
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
  rm -f "${TMP_ROOT}"/*-forward.pid "${TMP_ROOT}"/*-reverse.pid
}

shutdown_ports() {
  local port
  for port in "$@"; do
    redis-cli -p "${port}" shutdown nosave >/dev/null 2>&1 || true
  done
}

build_binaries() {
  echo "[1/8] building binaries"
  mkdir -p "${TMP_ROOT}/gocache"
  (cd "${ROOT}" && GOCACHE="${TMP_ROOT}/gocache" go build -o "${TMP_ROOT}/redisGunYu" ./main.go)
  (cd "${ROOT}" && GOCACHE="${TMP_ROOT}/gocache" go build -o "${TMP_ROOT}/bisync_compare" ./tests/bisync/cmd/bisync_compare)
  (cd "${ROOT}" && GOCACHE="${TMP_ROOT}/gocache" go build -o "${TMP_ROOT}/bisync_workload" ./tests/bisync/cmd/bisync_workload)
}

validate_redis_server_version() {
  local version
  local major
  version=$("${REDIS_SERVER_BIN}" --version 2>/dev/null || true)
  major=$(printf '%s\n' "${version}" | sed -n 's/.* v=\([0-9][0-9]*\)\..*/\1/p')
  if [[ -z "${major}" ]]; then
    echo "could not parse redis-server version from: ${version}" >&2
    return 1
  fi
  if (( major > 7 )) && [[ "${ALLOW_UNSUPPORTED_REDIS}" != "1" ]]; then
    echo "redis-server ${version} is not accepted for category9 by default; current RDB loader is validated for Redis <= 7. Set REDIS_SERVER_BIN to a Redis 7 binary, or set ALLOW_UNSUPPORTED_REDIS=1 for compatibility investigation." >&2
    return 1
  fi
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
appendonly yes
appendfsync everysec
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

wait_for_cluster_all_ok() {
  local port
  for port in "$@"; do
    wait_for_cluster_ok "${port}"
  done
}

start_cluster_with_replicas() {
  local prefix=$1
  shift
  local ports=("$@")
  local port

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
    "127.0.0.1:${ports[3]}" \
    "127.0.0.1:${ports[4]}" \
    "127.0.0.1:${ports[5]}" \
    --cluster-replicas 1 \
    --cluster-yes >/dev/null

  wait_for_cluster_all_ok "${ports[@]}"
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
  gracefullStopTimeout: 3s
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
    maxSize: 4294967296
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
  for _ in $(seq 1 120); do
    if curl -sf "http://127.0.0.1:${port}/syncer/status" >/dev/null; then
      return 0
    fi
    sleep 0.25
  done
  echo "syncer on http port ${port} did not become ready" >&2
  return 1
}

start_syncers() {
  local mode=$1
  local left_addrs=$2
  local right_addrs=$3
  local fwd_http_port=$4
  local rev_http_port=$5
  local mode_arg=$6
  local replay_mode
  replay_mode=$(normalize_replay_mode "${mode_arg}")

  write_syncer_conf "${mode}-forward" "${fwd_http_port}" "${left_addrs}" "${right_addrs}" "${mode_arg}"
  write_syncer_conf "${mode}-reverse" "${rev_http_port}" "${right_addrs}" "${left_addrs}" "${mode_arg}"

  "${TMP_ROOT}/redisGunYu" -conf "${TMP_ROOT}/${mode}-forward.yaml" -cmd sync > "${TMP_ROOT}/${mode}-forward.log" 2>&1 &
  FWD_PID=$!
  echo "${FWD_PID}" > "${TMP_ROOT}/${mode}-forward.pid"
  "${TMP_ROOT}/redisGunYu" -conf "${TMP_ROOT}/${mode}-reverse.yaml" -cmd sync > "${TMP_ROOT}/${mode}-reverse.log" 2>&1 &
  REV_PID=$!
  echo "${REV_PID}" > "${TMP_ROOT}/${mode}-reverse.pid"

  wait_for_syncer "${fwd_http_port}"
  wait_for_syncer "${rev_http_port}"
  sleep 5
}

restart_syncers_via_api() {
  local fwd_http_port=$1
  local rev_http_port=$2
  curl -sf -XPOST "http://127.0.0.1:${fwd_http_port}/syncer/restart" >/dev/null
  curl -sf -XPOST "http://127.0.0.1:${rev_http_port}/syncer/restart" >/dev/null
  wait_for_syncer "${fwd_http_port}"
  wait_for_syncer "${rev_http_port}"
  sleep 5
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
  for _ in $(seq 1 120); do
    if [[ "$(redis-cli -p "${port}" --raw role 2>/dev/null | head -n 1)" == "${expected}" ]]; then
      return 0
    fi
    sleep 0.25
  done
  echo "redis on port ${port} did not become ${expected}" >&2
  return 1
}

force_failover() {
  local replica_port=$1
  redis-cli -p "${replica_port}" cluster failover force >/dev/null
  wait_for_role "${replica_port}" master
  sleep 3
}

pid_rss_kb() {
  local pid=$1
  if [[ -z "${pid}" ]] || ! ps -p "${pid}" >/dev/null 2>&1; then
    echo 0
    return
  fi
  ps -o rss= -p "${pid}" 2>/dev/null | awk '{print $1 + 0}'
}

pid_cpu_pct() {
  local pid=$1
  if [[ -z "${pid}" ]] || ! ps -p "${pid}" >/dev/null 2>&1; then
    echo 0
    return
  fi
  ps -o %cpu= -p "${pid}" 2>/dev/null | awk '{print $1 + 0}'
}

http_goroutines() {
  local port=$1
  curl -sf "http://127.0.0.1:${port}/debug/pprof/goroutine?debug=1" 2>/dev/null | awk '/^goroutine profile: total/ {print $4 + 0; found=1; exit} END {if (!found) print 0}'
}

store_kb() {
  local dir=$1
  if [[ ! -d "${dir}" ]]; then
    echo 0
    return
  fi
  du -sk "${dir}" 2>/dev/null | awk '{print $1 + 0}'
}

pid_from_file() {
  local file=$1
  if [[ -f "${file}" ]]; then
    cat "${file}"
  else
    echo 0
  fi
}

redis_sum_info_field() {
  local ports_csv=$1
  local section=$2
  local field=$3
  local total=0
  local port
  IFS=',' read -r -a ports <<< "${ports_csv}"
  for port in "${ports[@]}"; do
    local value
    value=$(redis-cli -p "${port}" info "${section}" 2>/dev/null | awk -F: -v key="${field}" '$1 == key {gsub("\r", "", $2); print $2 + 0; exit}')
    total=$((total + ${value:-0}))
  done
  echo "${total}"
}

cluster_ok_count() {
  local ports_csv=$1
  local ok=0
  local port
  IFS=',' read -r -a ports <<< "${ports_csv}"
  for port in "${ports[@]}"; do
    if redis-cli -p "${port}" cluster info 2>/dev/null | rg -q '^cluster_state:ok'; then
      ok=$((ok + 1))
    fi
  done
  echo "${ok}"
}

monitor_loop() {
  local mode=$1
  local fwd_http_port=$2
  local rev_http_port=$3
  local left_ports_csv=$4
  local right_ports_csv=$5
  local samples_file=$6
  local fwd_store="${TMP_ROOT}/${mode}-forward-store"
  local rev_store="${TMP_ROOT}/${mode}-reverse-store"

  while true; do
    local ts
    local fwd_pid rev_pid
    ts=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
    fwd_pid=$(pid_from_file "${TMP_ROOT}/${mode}-forward.pid")
    rev_pid=$(pid_from_file "${TMP_ROOT}/${mode}-reverse.pid")
    printf '{"ts":"%s","mode":"%s","fwd_pid":%s,"rev_pid":%s,"fwd_rss_kb":%s,"rev_rss_kb":%s,"fwd_cpu_pct":%s,"rev_cpu_pct":%s,"fwd_goroutines":%s,"rev_goroutines":%s,"fwd_store_kb":%s,"rev_store_kb":%s,"left_used_memory":%s,"right_used_memory":%s,"left_evicted_keys":%s,"right_evicted_keys":%s,"left_cluster_ok_nodes":%s,"right_cluster_ok_nodes":%s}\n' \
      "${ts}" "${mode}" "${fwd_pid}" "${rev_pid}" \
      "$(pid_rss_kb "${fwd_pid}")" "$(pid_rss_kb "${rev_pid}")" \
      "$(pid_cpu_pct "${fwd_pid}")" "$(pid_cpu_pct "${rev_pid}")" \
      "$(http_goroutines "${fwd_http_port}")" "$(http_goroutines "${rev_http_port}")" \
      "$(store_kb "${fwd_store}")" "$(store_kb "${rev_store}")" \
      "$(redis_sum_info_field "${left_ports_csv}" memory used_memory)" \
      "$(redis_sum_info_field "${right_ports_csv}" memory used_memory)" \
      "$(redis_sum_info_field "${left_ports_csv}" stats evicted_keys)" \
      "$(redis_sum_info_field "${right_ports_csv}" stats evicted_keys)" \
      "$(cluster_ok_count "${left_ports_csv}")" "$(cluster_ok_count "${right_ports_csv}")" >> "${samples_file}"
    sleep "${MONITOR_INTERVAL_SECONDS}"
  done
}

record_event() {
  local file=$1
  local mode=$2
  local event=$3
  local status=$4
  local detail=$5
  printf '{"ts":"%s","mode":"%s","event":"%s","status":"%s","detail":"%s"}\n' \
    "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "${mode}" "${event}" "${status}" "${detail}" >> "${file}"
}

sleep_until_elapsed() {
  local started_at=$1
  local target_elapsed=$2
  local workload_pid=$3
  while ps -p "${workload_pid}" >/dev/null 2>&1; do
    local now elapsed remaining
    now=$(date +%s)
    elapsed=$((now - started_at))
    if (( elapsed >= target_elapsed )); then
      return 0
    fi
    remaining=$((target_elapsed - elapsed))
    if (( remaining > 30 )); then
      sleep 30
    else
      sleep "${remaining}"
    fi
  done
  echo "workload exited before scheduled fault at ${target_elapsed}s" >&2
  return 1
}

run_fault_schedule() {
  local mode=$1
  local duration_seconds=$2
  local workload_pid=$3
  local started_at=$4
  local fwd_http_port=$5
  local rev_http_port=$6
  local left_ports_csv=$7
  local right_ports_csv=$8
  local left_addrs=$9
  local right_addrs=${10}
  local mode_arg=${11}
  local replay_mode
  replay_mode=$(normalize_replay_mode "${mode_arg}")
  local events_file=${12}

  local t_restart=$((duration_seconds / 4))
  local t_failover=$((duration_seconds / 2))
  local t_offline=$((duration_seconds * 7 / 10))
  local t_api_restart=$((duration_seconds * 17 / 20))
  local left_ports right_ports replica
  IFS=',' read -r -a left_ports <<< "${left_ports_csv}"
  IFS=',' read -r -a right_ports <<< "${right_ports_csv}"

  sleep_until_elapsed "${started_at}" "${t_restart}" "${workload_pid}" || return 1
  record_event "${events_file}" "${mode}" "syncer_restart_api" "start" "both directions"
  restart_syncers_via_api "${fwd_http_port}" "${rev_http_port}"
  record_event "${events_file}" "${mode}" "syncer_restart_api" "done" "both directions"

  sleep_until_elapsed "${started_at}" "${t_failover}" "${workload_pid}" || return 1
  replica=$(find_first_replica_port "${left_ports[@]}")
  record_event "${events_file}" "${mode}" "left_failover" "start" "replica=${replica}"
  force_failover "${replica}"
  wait_for_cluster_all_ok "${left_ports[@]}"
  record_event "${events_file}" "${mode}" "left_failover" "done" "replica=${replica}"
  replica=$(find_first_replica_port "${right_ports[@]}")
  record_event "${events_file}" "${mode}" "right_failover" "start" "replica=${replica}"
  force_failover "${replica}"
  wait_for_cluster_all_ok "${right_ports[@]}"
  record_event "${events_file}" "${mode}" "right_failover" "done" "replica=${replica}"

  sleep_until_elapsed "${started_at}" "${t_offline}" "${workload_pid}" || return 1
  record_event "${events_file}" "${mode}" "syncer_offline_resume" "start" "offline_seconds=${SOAK_OFFLINE_SECONDS}"
  stop_syncers
  sleep "${SOAK_OFFLINE_SECONDS}"
  start_syncers "${mode}" "${left_addrs}" "${right_addrs}" "${fwd_http_port}" "${rev_http_port}" "${mode_arg}"
  record_event "${events_file}" "${mode}" "syncer_offline_resume" "done" "offline_seconds=${SOAK_OFFLINE_SECONDS}"

  sleep_until_elapsed "${started_at}" "${t_api_restart}" "${workload_pid}" || return 1
  record_event "${events_file}" "${mode}" "final_syncer_restart_api" "start" "both directions"
  restart_syncers_via_api "${fwd_http_port}" "${rev_http_port}"
  record_event "${events_file}" "${mode}" "final_syncer_restart_api" "done" "both directions"
}

wait_for_consistency() {
  local left_addrs=$1
  local right_addrs=$2
  local pattern=$3
  local compare_log=$4
  for _ in $(seq 1 900); do
    if "${TMP_ROOT}/bisync_compare" --left-addrs "${left_addrs}" --right-addrs "${right_addrs}" --pattern "${pattern}" > "${compare_log}" 2>&1; then
      sleep 5
      "${TMP_ROOT}/bisync_compare" --left-addrs "${left_addrs}" --right-addrs "${right_addrs}" --pattern "${pattern}" > "${compare_log}" 2>&1
      return 0
    fi
    sleep 2
  done
  cat "${compare_log}" >&2 || true
  return 1
}

assert_no_syncer_fatal_logs() {
  local mode=$1
  local fwd_log="${TMP_ROOT}/${mode}-forward.log"
  local rev_log="${TMP_ROOT}/${mode}-reverse.log"
  local rdb_errors
  if rg -q 'panic|fatal' "${fwd_log}" "${rev_log}" 2>/dev/null; then
    echo "fatal syncer log detected for ${mode}" >&2
    rg -n 'panic|fatal' "${fwd_log}" "${rev_log}" >&2 || true
    return 1
  fi
  rdb_errors=$(rg -n 'send rdb ERROR' "${fwd_log}" "${rev_log}" 2>/dev/null | rg -v 'context canceled' || true)
  if [[ -n "${rdb_errors}" ]]; then
    echo "non-cancelled RDB error detected for ${mode}" >&2
    printf '%s\n' "${rdb_errors}" >&2
    return 1
  fi
}

metric_value() {
  local file=$1
  local field=$2
  local which=$3
  local values
  values=$(sed -n "s/.*\"${field}\":\\([0-9.]*\\).*/\\1/p" "${file}")
  if [[ -z "${values}" ]]; then
    echo 0
    return
  fi
  case "${which}" in
    first) echo "${values}" | head -n 1 ;;
    last) echo "${values}" | tail -n 1 ;;
    max) echo "${values}" | sort -n | tail -n 1 ;;
  esac
}

growth_percent() {
  local first=$1
  local last=$2
  if [[ "${first}" == "0" ]]; then
    echo 0
    return
  fi
  awk -v first="${first}" -v last="${last}" 'BEGIN { printf "%.0f", ((last - first) * 100 / first) }'
}

resource_warning() {
  local samples_file=$1
  local fwd_rss_first fwd_rss_last rev_rss_first rev_rss_last
  local fwd_go_first fwd_go_last rev_go_first rev_go_last
  local fwd_rss_growth rev_rss_growth fwd_go_growth rev_go_growth
  fwd_rss_first=$(metric_value "${samples_file}" fwd_rss_kb first)
  fwd_rss_last=$(metric_value "${samples_file}" fwd_rss_kb last)
  rev_rss_first=$(metric_value "${samples_file}" rev_rss_kb first)
  rev_rss_last=$(metric_value "${samples_file}" rev_rss_kb last)
  fwd_go_first=$(metric_value "${samples_file}" fwd_goroutines first)
  fwd_go_last=$(metric_value "${samples_file}" fwd_goroutines last)
  rev_go_first=$(metric_value "${samples_file}" rev_goroutines first)
  rev_go_last=$(metric_value "${samples_file}" rev_goroutines last)
  fwd_rss_growth=$(growth_percent "${fwd_rss_first}" "${fwd_rss_last}")
  rev_rss_growth=$(growth_percent "${rev_rss_first}" "${rev_rss_last}")
  fwd_go_growth=$(growth_percent "${fwd_go_first}" "${fwd_go_last}")
  rev_go_growth=$(growth_percent "${rev_go_first}" "${rev_go_last}")

  if (( fwd_rss_growth > MAX_RSS_GROWTH_PERCENT || rev_rss_growth > MAX_RSS_GROWTH_PERCENT || fwd_go_growth > MAX_GOROUTINE_GROWTH_PERCENT || rev_go_growth > MAX_GOROUTINE_GROWTH_PERCENT )); then
    echo "YES"
  else
    echo "NO"
  fi
}

goroutine_warning() {
  local samples_file=$1
  local fwd_go_first fwd_go_last rev_go_first rev_go_last
  local fwd_go_growth rev_go_growth
  fwd_go_first=$(metric_value "${samples_file}" fwd_goroutines first)
  fwd_go_last=$(metric_value "${samples_file}" fwd_goroutines last)
  rev_go_first=$(metric_value "${samples_file}" rev_goroutines first)
  rev_go_last=$(metric_value "${samples_file}" rev_goroutines last)
  fwd_go_growth=$(growth_percent "${fwd_go_first}" "${fwd_go_last}")
  rev_go_growth=$(growth_percent "${rev_go_first}" "${rev_go_last}")
  if (( fwd_go_growth > MAX_GOROUTINE_GROWTH_PERCENT || rev_go_growth > MAX_GOROUTINE_GROWTH_PERCENT )); then
    echo "YES"
  else
    echo "NO"
  fi
}

dump_goroutines() {
  local mode=$1
  local fwd_http_port=$2
  local rev_http_port=$3
  local fwd_dump="${TMP_ROOT}/${mode}-forward-goroutines.txt"
  local rev_dump="${TMP_ROOT}/${mode}-reverse-goroutines.txt"
  curl -sf "http://127.0.0.1:${fwd_http_port}/debug/pprof/goroutine?debug=2" > "${fwd_dump}" 2>&1 || echo "failed to fetch forward goroutine dump" > "${fwd_dump}"
  curl -sf "http://127.0.0.1:${rev_http_port}/debug/pprof/goroutine?debug=2" > "${rev_dump}" 2>&1 || echo "failed to fetch reverse goroutine dump" > "${rev_dump}"
}

workload_report_table() {
  local workload_json=$1
  python3 - "$workload_json" <<'PY'
import json
import re
import sys
from datetime import datetime

path = sys.argv[1]
try:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
except Exception as exc:
    print(f"workload summary unavailable: {exc}")
    sys.exit(0)

def parse_ts(value):
    value = value.replace("Z", "+00:00")
    match = re.match(r"^(.*T\d\d:\d\d:\d\d)(?:\.(\d+))?([+-]\d\d:\d\d)?$", value)
    if not match:
        return datetime.fromisoformat(value)
    head, fraction, offset = match.groups()
    if fraction:
        fraction = (fraction + "000000")[:6]
        value = f"{head}.{fraction}{offset or ''}"
    return datetime.fromisoformat(value)

try:
    seconds = max((parse_ts(data["finished_at"]) - parse_ts(data["started_at"])).total_seconds(), 1.0)
except Exception:
    seconds = 1.0

print(f"- Scenario: {data.get('scenario', '')}")
print(f"- Prefix: {data.get('prefix', '')}")
print(f"- DurationSeconds: {seconds:.0f}")
print()
print("| Side | UniqueKeys | Iterations | TotalCommands | CommandsPerSecond | TransientRetries | ApproxPayloadBytes |")
print("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
for side in ("left", "right"):
    summary = data.get("sides", {}).get(side, {})
    commands = summary.get("commands", {}) or {}
    total = sum(int(v) for v in commands.values())
    print(
        f"| {side} | {int(summary.get('unique_keys', 0))} | "
        f"{int(summary.get('iterations', 0))} | {total} | {total / seconds:.2f} | "
        f"{int(summary.get('transient_retries', 0))} | "
        f"{int(summary.get('approx_payload_bytes', 0))} |"
    )
print()
print("| Side | Command | Count |")
print("| --- | --- | ---: |")
for side in ("left", "right"):
    commands = data.get("sides", {}).get(side, {}).get("commands", {}) or {}
    for cmd in sorted(commands):
        print(f"| {side} | {cmd} | {int(commands[cmd])} |")
PY
}

write_report() {
  local mode=$1
  local mode_arg=$2
  local replay_mode
  replay_mode=$(normalize_replay_mode "${mode_arg}")
  local prefix=$3
  local left_addrs=$4
  local right_addrs=$5
  local workload_json=$6
  local compare_log=$7
  local samples_file=$8
  local events_file=$9
  local report_file=${10}
  local fwd_http_port=${11}
  local rev_http_port=${12}
  local overall=${13}
  local status_fwd="${TMP_ROOT}/${mode}-forward-status.json"
  local status_rev="${TMP_ROOT}/${mode}-reverse-status.json"
  local fwd_goroutine_dump="${TMP_ROOT}/${mode}-forward-goroutines.txt"
  local rev_goroutine_dump="${TMP_ROOT}/${mode}-reverse-goroutines.txt"
  local warning
  local go_warning

  curl -sf "http://127.0.0.1:${fwd_http_port}/syncer/status" > "${status_fwd}" || true
  curl -sf "http://127.0.0.1:${rev_http_port}/syncer/status" > "${status_rev}" || true
  warning=$(resource_warning "${samples_file}")
  go_warning=$(goroutine_warning "${samples_file}")
  if [[ "${go_warning}" == "YES" ]]; then
    dump_goroutines "${mode}" "${fwd_http_port}" "${rev_http_port}"
  fi

  cat > "${report_file}" <<EOF
# Bisync Durability Report: Category 9

- GeneratedAt: $(date '+%Y-%m-%d %H:%M:%S %z')
- Tier: ${SOAK_TIER}
- Duration: ${SOAK_DURATION}
- Mode: ${mode}
- Mode: ${replay_mode}
- Prefix: ${prefix}
- StableComparePattern: ${prefix}:stable:*
- TargetQPS: ${SOAK_TARGET_QPS}
- Workers: ${SOAK_WORKERS}
- LeftCluster: ${left_addrs}
- RightCluster: ${right_addrs}
- Overall: ${overall}
- CompareResult: $(tr '\n' ' ' < "${compare_log}" | sed 's/[[:space:]]\+/ /g')
- ResourceWarning: ${warning}
- GoroutineWarning: ${go_warning}
- ForwardGoroutineDump: $(if [[ -f "${fwd_goroutine_dump}" ]]; then echo "${fwd_goroutine_dump}"; else echo "not generated"; fi)
- ReverseGoroutineDump: $(if [[ -f "${rev_goroutine_dump}" ]]; then echo "${rev_goroutine_dump}"; else echo "not generated"; fi)
- Samples: ${samples_file}
- FaultEvents: ${events_file}
- WorkloadJSON: ${workload_json}
- ForwardLog: ${TMP_ROOT}/${mode}-forward.log
- ReverseLog: ${TMP_ROOT}/${mode}-reverse.log

## Manual Gate

This script runs only the selected tier. Review this report before starting the next tier.
$(if [[ -n "${NEXT_TIER}" ]]; then echo "Next command: SOAK_TIER=${NEXT_TIER} SCENARIOS=${mode} KEEP_TMP=1 bash ./tests/bisync/run_category9.sh"; else echo "No next tier remains in the 2h/4h/6h sequence."; fi)

## Test Coverage

- Sustained bidirectional writes during the full duration.
- Stable business data compare on strings, counters, hashes, lists, sets, zsets, large strings, long keys, binary strings, and same-slot transactions.
- Volatile boundary writes for TTL and stream commands are included in the workload but excluded from final stable compare.
- Fault schedule: syncer API restart, left Redis failover, right Redis failover, syncer offline/resume while writes continue, final syncer API restart.
- Resource sampling: syncer RSS, CPU, goroutine count, storer size, Redis memory, Redis evictions, cluster health.

## Workload Metrics

$(workload_report_table "${workload_json}")

## Resource Summary

| Metric | First | Last | Max |
| --- | ---: | ---: | ---: |
| fwd_rss_kb | $(metric_value "${samples_file}" fwd_rss_kb first) | $(metric_value "${samples_file}" fwd_rss_kb last) | $(metric_value "${samples_file}" fwd_rss_kb max) |
| rev_rss_kb | $(metric_value "${samples_file}" rev_rss_kb first) | $(metric_value "${samples_file}" rev_rss_kb last) | $(metric_value "${samples_file}" rev_rss_kb max) |
| fwd_goroutines | $(metric_value "${samples_file}" fwd_goroutines first) | $(metric_value "${samples_file}" fwd_goroutines last) | $(metric_value "${samples_file}" fwd_goroutines max) |
| rev_goroutines | $(metric_value "${samples_file}" rev_goroutines first) | $(metric_value "${samples_file}" rev_goroutines last) | $(metric_value "${samples_file}" rev_goroutines max) |
| left_used_memory | $(metric_value "${samples_file}" left_used_memory first) | $(metric_value "${samples_file}" left_used_memory last) | $(metric_value "${samples_file}" left_used_memory max) |
| right_used_memory | $(metric_value "${samples_file}" right_used_memory first) | $(metric_value "${samples_file}" right_used_memory last) | $(metric_value "${samples_file}" right_used_memory max) |
| fwd_store_kb | $(metric_value "${samples_file}" fwd_store_kb first) | $(metric_value "${samples_file}" fwd_store_kb last) | $(metric_value "${samples_file}" fwd_store_kb max) |
| rev_store_kb | $(metric_value "${samples_file}" rev_store_kb first) | $(metric_value "${samples_file}" rev_store_kb last) | $(metric_value "${samples_file}" rev_store_kb max) |

## Fault Events

\`\`\`json
$(cat "${events_file}")
\`\`\`

## Workload Summary

\`\`\`json
$(cat "${workload_json}")
\`\`\`

## Forward Status

\`\`\`json
$(cat "${status_fwd}")
\`\`\`

## Reverse Status

\`\`\`json
$(cat "${status_rev}")
\`\`\`
EOF

  if [[ "${warning}" == "YES" && "${FAIL_ON_RESOURCE_WARNING}" == "1" ]]; then
    echo "resource warning exceeded thresholds; report=${report_file}" >&2
    return 1
  fi
}

run_scenario() {
  local mode=$1
  local mode_arg=$2
  local replay_mode
  replay_mode=$(normalize_replay_mode "${mode_arg}")
  local fwd_http_port=$3
  local rev_http_port=$4
  local src_base dst_base
  local src_ports dst_ports src_csv dst_csv src_port_csv dst_port_csv
  local prefix="${TEST_PREFIX}:${SOAK_TIER}:${mode}"
  local workload_json="${TMP_ROOT}/${mode}-workload.json"
  local workload_log="${TMP_ROOT}/${mode}-workload.log"
  local compare_log="${TMP_ROOT}/${mode}-compare.log"
  local samples_file="${TMP_ROOT}/${mode}-samples.jsonl"
  local events_file="${TMP_ROOT}/${mode}-events.jsonl"
  local report_file="${TMP_ROOT}/${mode}-report.md"
  local duration_seconds started_at
  local scenario_status="PASS"

  if ! replay_mode_uses_frontier "${replay_mode}"; then
    src_base="${SERIAL_SRC_BASE}"
    dst_base="${SERIAL_DST_BASE}"
  else
    src_base="${PIPELINE_SRC_BASE}"
    dst_base="${PIPELINE_DST_BASE}"
  fi
  src_ports=("${src_base}" "$((src_base + 1))" "$((src_base + 2))" "$((src_base + 3))" "$((src_base + 4))" "$((src_base + 5))")
  dst_ports=("${dst_base}" "$((dst_base + 1))" "$((dst_base + 2))" "$((dst_base + 3))" "$((dst_base + 4))" "$((dst_base + 5))")
  src_csv=$(printf "127.0.0.1:%s,127.0.0.1:%s,127.0.0.1:%s,127.0.0.1:%s,127.0.0.1:%s,127.0.0.1:%s" "${src_ports[0]}" "${src_ports[1]}" "${src_ports[2]}" "${src_ports[3]}" "${src_ports[4]}" "${src_ports[5]}")
  dst_csv=$(printf "127.0.0.1:%s,127.0.0.1:%s,127.0.0.1:%s,127.0.0.1:%s,127.0.0.1:%s,127.0.0.1:%s" "${dst_ports[0]}" "${dst_ports[1]}" "${dst_ports[2]}" "${dst_ports[3]}" "${dst_ports[4]}" "${dst_ports[5]}")
  src_port_csv=$(join_csv "${src_ports[@]}")
  dst_port_csv=$(join_csv "${dst_ports[@]}")
  duration_seconds=$(duration_to_seconds "${SOAK_DURATION}")

  start_cluster_with_replicas "${mode}-src" "${src_ports[@]}"
  start_cluster_with_replicas "${mode}-dst" "${dst_ports[@]}"

  echo "[3/8] starting bisync syncers for ${mode}"
  start_syncers "${mode}" "${src_csv}" "${dst_csv}" "${fwd_http_port}" "${rev_http_port}" "${mode_arg}"

  echo "[4/8] starting resource monitor for ${mode}"
  : > "${samples_file}"
  : > "${events_file}"
  monitor_loop "${mode}" "${fwd_http_port}" "${rev_http_port}" "${src_port_csv}" "${dst_port_csv}" "${samples_file}" &
  MONITOR_PID=$!

  echo "[5/8] running durability workload for ${mode}, tier=${SOAK_TIER}, duration=${SOAK_DURATION}"
  "${TMP_ROOT}/bisync_workload" \
    --scenario soak \
    --left-addrs "${src_csv}" \
    --right-addrs "${dst_csv}" \
    --prefix "${prefix}" \
    --duration "${SOAK_DURATION}" \
    --key-space "${SOAK_KEY_SPACE}" \
    --throttle "${SOAK_THROTTLE}" \
    --target-qps "${SOAK_TARGET_QPS}" \
    --workers "${SOAK_WORKERS}" \
    --boundary-every "${SOAK_BOUNDARY_EVERY}" \
    --volatile-every "${SOAK_VOLATILE_EVERY}" \
    --txn-every "${SOAK_TXN_EVERY}" \
    --report-json "${workload_json}" > "${workload_log}" 2>&1 &
  WORKLOAD_PID=$!
  started_at=$(date +%s)

  echo "[6/8] injecting scheduled faults for ${mode}"
  if ! run_fault_schedule "${mode}" "${duration_seconds}" "${WORKLOAD_PID}" "${started_at}" "${fwd_http_port}" "${rev_http_port}" "${src_port_csv}" "${dst_port_csv}" "${src_csv}" "${dst_csv}" "${mode_arg}" "${events_file}"; then
    scenario_status="FAIL"
    record_event "${events_file}" "${mode}" "fault_schedule" "failed" "see logs"
  fi

  if ! wait "${WORKLOAD_PID}"; then
    scenario_status="FAIL"
    record_event "${events_file}" "${mode}" "workload" "failed" "see ${workload_log}"
  fi
  WORKLOAD_PID=""

  echo "[7/8] waiting for final convergence for ${mode}"
  sleep "${SOAK_FINAL_SETTLE_SECONDS}"
  if ! wait_for_cluster_all_ok "${src_ports[@]}" "${dst_ports[@]}"; then
    scenario_status="FAIL"
    echo "cluster did not recover to ok" > "${compare_log}"
  elif ! assert_no_syncer_fatal_logs "${mode}"; then
    scenario_status="FAIL"
    echo "fatal syncer log detected; compare skipped" > "${compare_log}"
  elif ! wait_for_consistency "${src_csv}" "${dst_csv}" "${prefix}:stable:*" "${compare_log}"; then
    scenario_status="FAIL"
  fi

  if [[ -n "${MONITOR_PID}" ]]; then
    kill "${MONITOR_PID}" >/dev/null 2>&1 || true
    wait "${MONITOR_PID}" >/dev/null 2>&1 || true
    MONITOR_PID=""
  fi

  echo "[8/8] writing report for ${mode}"
  [[ -f "${workload_json}" ]] || echo "{}" > "${workload_json}"
  [[ -f "${compare_log}" ]] || echo "compare did not run" > "${compare_log}"
  write_report "${mode}" "${mode_arg}" "${prefix}" "${src_csv}" "${dst_csv}" "${workload_json}" "${compare_log}" "${samples_file}" "${events_file}" "${report_file}" "${fwd_http_port}" "${rev_http_port}" "${scenario_status}"
  echo "report=${report_file}"
  echo "samples=${samples_file}"
  echo "events=${events_file}"

  stop_syncers
  shutdown_ports "${src_ports[@]}" "${dst_ports[@]}"
  if [[ "${scenario_status}" != "PASS" ]]; then
    return 1
  fi
}

validate_redis_server_version
rm -rf "${TMP_ROOT}"
mkdir -p "${TMP_ROOT}"
shutdown_ports $(all_ports)
build_binaries

case ",${SCENARIOS}," in
  *",sync,"*)
    run_scenario sync sync "${SERIAL_HTTP_PORT}" "${SERIAL_REV_HTTP_PORT}"
    ;;
esac
case ",${SCENARIOS}," in
  *",pipeline,"*)
    run_scenario pipeline pipeline "${SERIAL_HTTP_PORT}" "${SERIAL_REV_HTTP_PORT}"
    ;;
esac
case ",${SCENARIOS}," in
  *",parallel,"*)
    run_scenario parallel parallel "${PIPELINE_FWD_HTTP_PORT}" "${PIPELINE_REV_HTTP_PORT}"
    ;;
esac
