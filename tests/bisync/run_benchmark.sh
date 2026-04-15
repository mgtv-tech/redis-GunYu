#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_ROOT="${TMP_ROOT:-${TMPDIR:-/tmp}/redisgunyu-bisync-benchmark}"
source "${ROOT}/tests/bisync/lib/redis_env.sh"

TEST_PREFIX="${TEST_PREFIX:-bisync:bench:$(date +%s)}"
SCENARIOS="${SCENARIOS:-serial,pipeline}"
BENCH_DURATION="${BENCH_DURATION:-15m}"
BENCH_KEY_SPACE="${BENCH_KEY_SPACE:-100000}"
BENCH_TARGET_QPS_LIST="${BENCH_TARGET_QPS_LIST:-1000,5000,10000}"
BENCH_WORKERS="${BENCH_WORKERS:-4}"
BENCH_THROTTLE="${BENCH_THROTTLE:-0ms}"
BENCH_BOUNDARY_EVERY="${BENCH_BOUNDARY_EVERY:-5000}"
BENCH_VOLATILE_EVERY="${BENCH_VOLATILE_EVERY:-0}"
BENCH_TXN_EVERY="${BENCH_TXN_EVERY:-500}"
BENCH_COMPARE_MAX_KEYS="${BENCH_COMPARE_MAX_KEYS:-0}"
BENCH_FINAL_SETTLE_SECONDS="${BENCH_FINAL_SETTLE_SECONDS:-15}"
SYNC_DELAY_TEST_KEY_PREFIX="${SYNC_DELAY_TEST_KEY_PREFIX:-redis-GunYu-bisync-benchmark-syncDelay}"
SYNC_DELAY_SAMPLE_INTERVAL_SECONDS="${SYNC_DELAY_SAMPLE_INTERVAL_SECONDS:-1}"
SYNC_DELAY_MAX_MS="${SYNC_DELAY_MAX_MS:-0}"

LEFT_PORTS=("${LEFT_PORT_1:-7000}" "${LEFT_PORT_2:-7001}" "${LEFT_PORT_3:-7002}")
RIGHT_PORTS=("${RIGHT_PORT_1:-7100}" "${RIGHT_PORT_2:-7101}" "${RIGHT_PORT_3:-7102}")
LEFT_ADDRS="${LEFT_ADDRS:-}"
RIGHT_ADDRS="${RIGHT_ADDRS:-}"
FWD_PID=""
REV_PID=""
RESOURCE_MONITOR_PID=""
SYNC_DELAY_MONITOR_PID=""

if [[ -z "${LEFT_ADDRS}" && -n "${LEFT_REDIS_DEPLOY_ROOT:-}" ]]; then
  LEFT_ADDRS="$(resolve_deploy_addrs LEFT_REDIS_DEPLOY_ROOT LEFT_REDIS_HOST)"
fi
if [[ -z "${RIGHT_ADDRS}" && -n "${RIGHT_REDIS_DEPLOY_ROOT:-}" ]]; then
  RIGHT_ADDRS="$(resolve_deploy_addrs RIGHT_REDIS_DEPLOY_ROOT RIGHT_REDIS_HOST)"
fi
if [[ -z "${LEFT_ADDRS}" ]]; then
  LEFT_ADDRS="127.0.0.1:${LEFT_PORTS[0]},127.0.0.1:${LEFT_PORTS[1]},127.0.0.1:${LEFT_PORTS[2]}"
fi
if [[ -z "${RIGHT_ADDRS}" ]]; then
  RIGHT_ADDRS="127.0.0.1:${RIGHT_PORTS[0]},127.0.0.1:${RIGHT_PORTS[1]},127.0.0.1:${RIGHT_PORTS[2]}"
fi
LEFT_PORTS=()
while IFS= read -r port; do
  LEFT_PORTS+=("${port}")
done < <(ports_from_addrs "${LEFT_ADDRS}")
RIGHT_PORTS=()
while IFS= read -r port; do
  RIGHT_PORTS+=("${port}")
done < <(ports_from_addrs "${RIGHT_ADDRS}")

cleanup() {
  local code=$?
  set +e
  stop_monitors
  stop_syncers
  exit "${code}"
}
trap cleanup EXIT

stop_monitors() {
  if [[ -n "${RESOURCE_MONITOR_PID}" ]]; then
    kill "${RESOURCE_MONITOR_PID}" >/dev/null 2>&1 || true
    wait "${RESOURCE_MONITOR_PID}" >/dev/null 2>&1 || true
    RESOURCE_MONITOR_PID=""
  fi
  if [[ -n "${SYNC_DELAY_MONITOR_PID}" ]]; then
    kill "${SYNC_DELAY_MONITOR_PID}" >/dev/null 2>&1 || true
    wait "${SYNC_DELAY_MONITOR_PID}" >/dev/null 2>&1 || true
    SYNC_DELAY_MONITOR_PID=""
  fi
}

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

rm -rf "${TMP_ROOT}"
mkdir -p "${TMP_ROOT}"

build_binaries() {
  echo "[1/7] building benchmark binaries"
  mkdir -p "${TMP_ROOT}/gocache"
  (cd "${ROOT}" && GOCACHE="${TMP_ROOT}/gocache" go build -o "${TMP_ROOT}/redisGunYu" ./main.go)
  (cd "${ROOT}" && GOCACHE="${TMP_ROOT}/gocache" go build -o "${TMP_ROOT}/bisync_compare" ./tests/bisync/cmd/bisync_compare)
  (cd "${ROOT}" && GOCACHE="${TMP_ROOT}/gocache" go build -o "${TMP_ROOT}/bisync_workload" ./tests/bisync/cmd/bisync_workload)
}

flush_ports() {
  local port
  for port in "$@"; do
    redis-cli -p "${port}" flushall async >/dev/null
  done
}

wait_for_empty_ports() {
  local ports=("$@")
  local port dbsize
  for _ in $(seq 1 60); do
    local all_zero=1
    for port in "${ports[@]}"; do
      dbsize=$(redis-cli -p "${port}" dbsize 2>/dev/null || echo "1")
      if [[ "${dbsize}" != "0" ]]; then
        all_zero=0
        break
      fi
    done
    if [[ "${all_zero}" == "1" ]]; then
      return 0
    fi
    sleep 0.5
  done
  echo "ports did not become empty: ${ports[*]}" >&2
  return 1
}

clear_clusters() {
  echo "[2/7] flushing benchmark clusters"
  flush_ports "${LEFT_PORTS[@]}" "${RIGHT_PORTS[@]}"
  wait_for_empty_ports "${LEFT_PORTS[@]}" "${RIGHT_PORTS[@]}"
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
  gracefullStopTimeout: 3s
  checkRedisTypologyTicker: 2s
input:
  redis:
    addresses: [${input_addrs}]
    type: cluster
    version: "7.0.11"
  mode: dynamic
  syncFrom: master
  syncDelayTestKey: ${SYNC_DELAY_TEST_KEY_PREFIX}:${name}
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
  local fwd_http_port=$2
  local rev_http_port=$3
  local pipeline_flag=$4

  echo "[3/7] starting benchmark syncers for ${mode}"
  write_syncer_conf "${mode}-forward" "${fwd_http_port}" "${LEFT_ADDRS}" "${RIGHT_ADDRS}" "${pipeline_flag}"
  write_syncer_conf "${mode}-reverse" "${rev_http_port}" "${RIGHT_ADDRS}" "${LEFT_ADDRS}" "${pipeline_flag}"

  "${TMP_ROOT}/redisGunYu" -conf "${TMP_ROOT}/${mode}-forward.yaml" -cmd sync > "${TMP_ROOT}/${mode}-forward.log" 2>&1 &
  FWD_PID=$!
  "${TMP_ROOT}/redisGunYu" -conf "${TMP_ROOT}/${mode}-reverse.yaml" -cmd sync > "${TMP_ROOT}/${mode}-reverse.log" 2>&1 &
  REV_PID=$!

  wait_for_syncer "${fwd_http_port}"
  wait_for_syncer "${rev_http_port}"
  sleep 5
}

wait_for_cluster_reachable() {
  local ports=("$@")
  local port
  for _ in $(seq 1 60); do
    local all_ok=1
    for port in "${ports[@]}"; do
      if ! redis-cli -p "${port}" ping >/dev/null 2>&1; then
        all_ok=0
        break
      fi
    done
    if [[ "${all_ok}" == "1" ]]; then
      return 0
    fi
    sleep 2
  done
  echo "clusters did not become reachable: ${ports[*]}" >&2
  return 1
}

wait_for_consistency() {
  local pattern=$1
  local compare_log=$2
  for _ in $(seq 1 300); do
    if "${TMP_ROOT}/bisync_compare" --left-addrs "${LEFT_ADDRS}" --right-addrs "${RIGHT_ADDRS}" --pattern "${pattern}" --max-keys "${BENCH_COMPARE_MAX_KEYS}" > "${compare_log}" 2>&1; then
      sleep 2
      "${TMP_ROOT}/bisync_compare" --left-addrs "${LEFT_ADDRS}" --right-addrs "${RIGHT_ADDRS}" --pattern "${pattern}" --max-keys "${BENCH_COMPARE_MAX_KEYS}" > "${compare_log}" 2>&1
      return 0
    fi
    sleep 1
  done
  cat "${compare_log}" >&2 || true
  return 1
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

resource_monitor_loop() {
  local mode=$1
  local fwd_http_port=$2
  local rev_http_port=$3
  local samples_file=$4
  local fwd_store="${TMP_ROOT}/${mode}-forward-store"
  local rev_store="${TMP_ROOT}/${mode}-reverse-store"
  while true; do
    printf '{"ts":"%s","mode":"%s","fwd_pid":%s,"rev_pid":%s,"fwd_rss_kb":%s,"rev_rss_kb":%s,"fwd_cpu_pct":%s,"rev_cpu_pct":%s,"fwd_goroutines":%s,"rev_goroutines":%s,"fwd_store_kb":%s,"rev_store_kb":%s}\n' \
      "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "${mode}" "${FWD_PID:-0}" "${REV_PID:-0}" \
      "$(pid_rss_kb "${FWD_PID:-0}")" "$(pid_rss_kb "${REV_PID:-0}")" \
      "$(pid_cpu_pct "${FWD_PID:-0}")" "$(pid_cpu_pct "${REV_PID:-0}")" \
      "$(http_goroutines "${fwd_http_port}")" "$(http_goroutines "${rev_http_port}")" \
      "$(store_kb "${fwd_store}")" "$(store_kb "${rev_store}")" >> "${samples_file}"
    sleep 5
  done
}

sample_metric_value() {
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

workload_report_table() {
  local workload_json=$1
  python3 - "${workload_json}" <<'PY'
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
PY
}

write_report() {
  local mode=$1
  local pipeline_flag=$2
  local target_qps=$3
  local prefix=$4
  local fwd_http_port=$5
  local rev_http_port=$6
  local workload_json=$7
  local compare_log=$8
  local resource_samples=$9
  local sync_delay_samples=${10}
  local report_file=${11}
  local status_fwd="${TMP_ROOT}/${mode}-qps${target_qps}-forward-status.json"
  local status_rev="${TMP_ROOT}/${mode}-qps${target_qps}-reverse-status.json"
  local sync_delay_status

  curl -sf "http://127.0.0.1:${fwd_http_port}/syncer/status" > "${status_fwd}" || true
  curl -sf "http://127.0.0.1:${rev_http_port}/syncer/status" > "${status_rev}" || true
  sync_delay_status=$(sync_delay_threshold_status "${sync_delay_samples}" "${SYNC_DELAY_MAX_MS}")

  cat > "${report_file}" <<EOF
# Bisync Benchmark Report

- GeneratedAt: $(date '+%Y-%m-%d %H:%M:%S %z')
- Mode: ${mode}
- Pipeline: ${pipeline_flag}
- TargetQPS: ${target_qps}
- Workers: ${BENCH_WORKERS}
- Duration: ${BENCH_DURATION}
- KeySpace: ${BENCH_KEY_SPACE}
- Prefix: ${prefix}
- SyncDelayMaxMs: ${SYNC_DELAY_MAX_MS}
- SyncDelayStatus: ${sync_delay_status}
- LeftCluster: ${LEFT_ADDRS}
- RightCluster: ${RIGHT_ADDRS}
- CompareResult: $(tr '\n' ' ' < "${compare_log}" | sed 's/[[:space:]]\+/ /g')
- ResourceSamples: ${resource_samples}
- SyncDelaySamples: ${sync_delay_samples}
- WorkloadJSON: ${workload_json}
- CompareMaxKeys: ${BENCH_COMPARE_MAX_KEYS}
- ForwardLog: ${TMP_ROOT}/${mode}-qps${target_qps}-forward.log
- ReverseLog: ${TMP_ROOT}/${mode}-qps${target_qps}-reverse.log

## Throughput

$(workload_report_table "${workload_json}")

## Sync Delay

$(sync_delay_report_table "${sync_delay_samples}")

## Resource Summary

| Metric | First | Last | Max |
| --- | ---: | ---: | ---: |
| fwd_rss_kb | $(sample_metric_value "${resource_samples}" fwd_rss_kb first) | $(sample_metric_value "${resource_samples}" fwd_rss_kb last) | $(sample_metric_value "${resource_samples}" fwd_rss_kb max) |
| rev_rss_kb | $(sample_metric_value "${resource_samples}" rev_rss_kb first) | $(sample_metric_value "${resource_samples}" rev_rss_kb last) | $(sample_metric_value "${resource_samples}" rev_rss_kb max) |
| fwd_cpu_pct | $(sample_metric_value "${resource_samples}" fwd_cpu_pct first) | $(sample_metric_value "${resource_samples}" fwd_cpu_pct last) | $(sample_metric_value "${resource_samples}" fwd_cpu_pct max) |
| rev_cpu_pct | $(sample_metric_value "${resource_samples}" rev_cpu_pct first) | $(sample_metric_value "${resource_samples}" rev_cpu_pct last) | $(sample_metric_value "${resource_samples}" rev_cpu_pct max) |
| fwd_goroutines | $(sample_metric_value "${resource_samples}" fwd_goroutines first) | $(sample_metric_value "${resource_samples}" fwd_goroutines last) | $(sample_metric_value "${resource_samples}" fwd_goroutines max) |
| rev_goroutines | $(sample_metric_value "${resource_samples}" rev_goroutines first) | $(sample_metric_value "${resource_samples}" rev_goroutines last) | $(sample_metric_value "${resource_samples}" rev_goroutines max) |
| fwd_store_kb | $(sample_metric_value "${resource_samples}" fwd_store_kb first) | $(sample_metric_value "${resource_samples}" fwd_store_kb last) | $(sample_metric_value "${resource_samples}" fwd_store_kb max) |
| rev_store_kb | $(sample_metric_value "${resource_samples}" rev_store_kb first) | $(sample_metric_value "${resource_samples}" rev_store_kb last) | $(sample_metric_value "${resource_samples}" rev_store_kb max) |

## Forward Status

\`\`\`json
$(cat "${status_fwd}")
\`\`\`

## Reverse Status

\`\`\`json
$(cat "${status_rev}")
\`\`\`
EOF
}

run_benchmark_case() {
  local mode=$1
  local pipeline_flag=$2
  local fwd_http_port=$3
  local rev_http_port=$4
  local target_qps=$5
  local prefix="${TEST_PREFIX}:${mode}:qps${target_qps}"
  local case_name="${mode}-qps${target_qps}"
  local workload_json="${TMP_ROOT}/${case_name}-workload.json"
  local compare_log="${TMP_ROOT}/${case_name}-compare.log"
  local resource_samples="${TMP_ROOT}/${case_name}-resources.jsonl"
  local sync_delay_samples="${TMP_ROOT}/${case_name}-sync-delay.jsonl"
  local report_file="${TMP_ROOT}/${case_name}-report.md"
  local sync_delay_status

  clear_clusters
  start_syncers "${case_name}" "${fwd_http_port}" "${rev_http_port}" "${pipeline_flag}"
  : > "${resource_samples}"
  : > "${sync_delay_samples}"
  resource_monitor_loop "${case_name}" "${fwd_http_port}" "${rev_http_port}" "${resource_samples}" &
  RESOURCE_MONITOR_PID=$!
  monitor_sync_delay_loop "${case_name}" "${fwd_http_port}" "${rev_http_port}" "${sync_delay_samples}" "${SYNC_DELAY_SAMPLE_INTERVAL_SECONDS}" &
  SYNC_DELAY_MONITOR_PID=$!

  echo "[4/7] running benchmark workload mode=${mode} target_qps=${target_qps}"
  "${TMP_ROOT}/bisync_workload" \
    --scenario soak \
    --left-addrs "${LEFT_ADDRS}" \
    --right-addrs "${RIGHT_ADDRS}" \
    --prefix "${prefix}" \
    --duration "${BENCH_DURATION}" \
    --key-space "${BENCH_KEY_SPACE}" \
    --throttle "${BENCH_THROTTLE}" \
    --target-qps "${target_qps}" \
    --workers "${BENCH_WORKERS}" \
    --boundary-every "${BENCH_BOUNDARY_EVERY}" \
    --volatile-every "${BENCH_VOLATILE_EVERY}" \
    --txn-every "${BENCH_TXN_EVERY}" \
    --report-json "${workload_json}" > "${TMP_ROOT}/${case_name}-workload.log" 2>&1

  echo "[5/7] waiting for convergence mode=${mode} target_qps=${target_qps}"
  sleep "${BENCH_FINAL_SETTLE_SECONDS}"
  wait_for_cluster_reachable "${LEFT_PORTS[@]}" "${RIGHT_PORTS[@]}"
  wait_for_consistency "${prefix}:stable:*" "${compare_log}"
  stop_monitors

  echo "[6/7] writing benchmark report mode=${mode} target_qps=${target_qps}"
  write_report "${mode}" "${pipeline_flag}" "${target_qps}" "${prefix}" "${fwd_http_port}" "${rev_http_port}" "${workload_json}" "${compare_log}" "${resource_samples}" "${sync_delay_samples}" "${report_file}"

  echo "[7/7] benchmark summary mode=${mode} target_qps=${target_qps}"
  echo "report=${report_file}"
  echo "workload_log=${TMP_ROOT}/${case_name}-workload.log"
  echo "compare_log=${compare_log}"
  echo "resource_samples=${resource_samples}"
  echo "sync_delay_samples=${sync_delay_samples}"

  stop_syncers
  sync_delay_status=$(sync_delay_threshold_status "${sync_delay_samples}" "${SYNC_DELAY_MAX_MS}")
  if [[ "${sync_delay_status}" == FAIL* ]]; then
    echo "sync delay check failed: ${sync_delay_status}" >&2
    return 1
  fi
}

run_mode() {
  local mode=$1
  local pipeline_flag=$2
  local fwd_http_port=$3
  local rev_http_port=$4
  local target_qps
  IFS=',' read -r -a qps_values <<< "${BENCH_TARGET_QPS_LIST}"
  for target_qps in "${qps_values[@]}"; do
    target_qps=$(printf '%s' "${target_qps}" | xargs)
    [[ -n "${target_qps}" ]] || continue
    run_benchmark_case "${mode}" "${pipeline_flag}" "${fwd_http_port}" "${rev_http_port}" "${target_qps}"
  done
}

build_binaries
case ",${SCENARIOS}," in
  *",serial,"*)
    run_mode serial false 21180 21280
    ;;
esac
case ",${SCENARIOS}," in
  *",pipeline,"*)
    run_mode pipeline true 21380 21480
    ;;
esac

echo "benchmark reports are under ${TMP_ROOT}"
