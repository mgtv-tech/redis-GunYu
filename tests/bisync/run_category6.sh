#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_ROOT="${TMPDIR:-/tmp}/redisgunyu-bisync-cat6"
source "${ROOT}/tests/bisync/lib/redis_env.sh"
TEST_PREFIX="${TEST_PREFIX:-bisync:cat6:$(date +%s)}"
SCENARIOS="${SCENARIOS:-serial,pipeline}"
LEFT_PORTS=("${LEFT_PORT_1:-7000}" "${LEFT_PORT_2:-7001}" "${LEFT_PORT_3:-7002}")
RIGHT_PORTS=("${RIGHT_PORT_1:-7100}" "${RIGHT_PORT_2:-7101}" "${RIGHT_PORT_3:-7102}")
LEFT_ADDRS="${LEFT_ADDRS:-}"
RIGHT_ADDRS="${RIGHT_ADDRS:-}"
FWD_PID=""
REV_PID=""

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
  stop_syncers
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

rm -rf "${TMP_ROOT}"
mkdir -p "${TMP_ROOT}"

build_binaries() {
  echo "[1/7] building binaries"
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
  local port
  local dbsize
  for _ in $(seq 1 40); do
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
  echo "[2/7] flushing both clusters"
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
    maxSize: 1073741824
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

wait_for_aof_ready() {
  local log_file=$1
  shift
  local ports=("$@")
  local port
  local ready

  for _ in $(seq 1 180); do
    ready=1
    for port in "${ports[@]}"; do
      if ! grep -Eq "\\[RedisOutput\\([^)]*:${port}\\)\\] send aof :" "${log_file}" 2>/dev/null; then
        ready=0
        break
      fi
    done
    if [[ "${ready}" == "1" ]]; then
      return 0
    fi
    sleep 0.5
  done

  echo "syncer did not enter AOF for all input shards: log=${log_file}, ports=${ports[*]}" >&2
  tail -n 120 "${log_file}" >&2 || true
  return 1
}

start_syncers() {
  local mode=$1
  local fwd_http_port=$2
  local rev_http_port=$3
  local pipeline_flag=$4

  echo "[3/7] starting bisync syncers for ${mode}"
  write_syncer_conf "${mode}-forward" "${fwd_http_port}" "${LEFT_ADDRS}" "${RIGHT_ADDRS}" "${pipeline_flag}"
  write_syncer_conf "${mode}-reverse" "${rev_http_port}" "${RIGHT_ADDRS}" "${LEFT_ADDRS}" "${pipeline_flag}"

  "${TMP_ROOT}/redisGunYu" -conf "${TMP_ROOT}/${mode}-forward.yaml" -cmd sync > "${TMP_ROOT}/${mode}-forward.log" 2>&1 &
  FWD_PID=$!
  "${TMP_ROOT}/redisGunYu" -conf "${TMP_ROOT}/${mode}-reverse.yaml" -cmd sync > "${TMP_ROOT}/${mode}-reverse.log" 2>&1 &
  REV_PID=$!

  wait_for_syncer "${fwd_http_port}"
  wait_for_syncer "${rev_http_port}"
  wait_for_aof_ready "${TMP_ROOT}/${mode}-forward.log" "${LEFT_PORTS[@]}"
  wait_for_aof_ready "${TMP_ROOT}/${mode}-reverse.log" "${RIGHT_PORTS[@]}"
}

wait_for_consistency() {
  local pattern=$1
  local compare_log=$2
  for _ in $(seq 1 120); do
    if "${TMP_ROOT}/bisync_compare" --left-addrs "${LEFT_ADDRS}" --right-addrs "${RIGHT_ADDRS}" --pattern "${pattern}" > "${compare_log}" 2>&1; then
      sleep 2
      "${TMP_ROOT}/bisync_compare" --left-addrs "${LEFT_ADDRS}" --right-addrs "${RIGHT_ADDRS}" --pattern "${pattern}" > "${compare_log}" 2>&1
      return 0
    fi
    sleep 1
  done
  cat "${compare_log}" >&2 || true
  return 1
}

write_report() {
  local mode=$1
  local pipeline_flag=$2
  local prefix=$3
  local fwd_http_port=$4
  local rev_http_port=$5
  local workload_json=$6
  local compare_log=$7
  local report_file=$8
  local status_fwd="${TMP_ROOT}/${mode}-forward-status.json"
  local status_rev="${TMP_ROOT}/${mode}-reverse-status.json"

  curl -sf "http://127.0.0.1:${fwd_http_port}/syncer/status" > "${status_fwd}"
  curl -sf "http://127.0.0.1:${rev_http_port}/syncer/status" > "${status_rev}"

  cat > "${report_file}" <<EOF
# Bisync Integration Report: Category 6

- GeneratedAt: $(date '+%Y-%m-%d %H:%M:%S %z')
- Mode: ${mode}
- Pipeline: ${pipeline_flag}
- Scenario: structures
- Prefix: ${prefix}
- LeftCluster: ${LEFT_ADDRS}
- RightCluster: ${RIGHT_ADDRS}
- CompareResult: $(tr '\n' ' ' < "${compare_log}" | sed 's/[[:space:]]\+/ /g')
- ForwardLog: ${TMP_ROOT}/${mode}-forward.log
- ReverseLog: ${TMP_ROOT}/${mode}-reverse.log

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
}

run_scenario() {
  local mode=$1
  local pipeline_flag=$2
  local fwd_http_port=$3
  local rev_http_port=$4
  local prefix="${TEST_PREFIX}:${mode}"
  local workload_json="${TMP_ROOT}/${mode}-workload.json"
  local compare_log="${TMP_ROOT}/${mode}-compare.log"
  local report_file="${TMP_ROOT}/${mode}-report.md"

  clear_clusters
  start_syncers "${mode}" "${fwd_http_port}" "${rev_http_port}" "${pipeline_flag}"

  echo "[4/7] writing mixed structures and large structures for ${mode}"
  "${TMP_ROOT}/bisync_workload" \
    --scenario structures \
    --left-addrs "${LEFT_ADDRS}" \
    --right-addrs "${RIGHT_ADDRS}" \
    --prefix "${prefix}" \
    --report-json "${workload_json}" > "${TMP_ROOT}/${mode}-workload.log"

  echo "[5/7] waiting for cluster convergence for ${mode}"
  wait_for_consistency "${prefix}:*" "${compare_log}"

  echo "[6/7] generating report for ${mode}"
  write_report "${mode}" "${pipeline_flag}" "${prefix}" "${fwd_http_port}" "${rev_http_port}" "${workload_json}" "${compare_log}" "${report_file}"

  echo "[7/7] summary ${mode}"
  echo "prefix=${prefix}"
  echo "report=${report_file}"
  echo "workload_log=${TMP_ROOT}/${mode}-workload.log"
  echo "compare_log=${compare_log}"

  stop_syncers
}

build_binaries
case ",${SCENARIOS}," in
  *",serial,"*)
    run_scenario serial false 19780 19880
    ;;
esac
case ",${SCENARIOS}," in
  *",pipeline,"*)
    run_scenario pipeline true 19980 20080
    ;;
esac
