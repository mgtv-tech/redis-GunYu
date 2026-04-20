#!/usr/bin/env bash

redis_server_candidates_from_root() {
  local root=$1
  printf '%s\n' \
    "${root}/src/redis-server" \
    "${root}/bin/redis-server" \
    "${root}/redis-server"
}

resolve_redis_server_bin() {
  local bin_env_name=$1
  local root_env_name=$2
  local explicit_bin=${!bin_env_name:-}
  local deploy_root=${!root_env_name:-}
  local candidate

  if [[ -n "${explicit_bin}" ]]; then
    if [[ -x "${explicit_bin}" ]]; then
      printf '%s\n' "${explicit_bin}"
      return 0
    fi
    echo "${bin_env_name} points to a non-executable file: ${explicit_bin}" >&2
    return 1
  fi

  if [[ -n "${deploy_root}" ]]; then
    while IFS= read -r candidate; do
      if [[ -x "${candidate}" ]]; then
        printf '%s\n' "${candidate}"
        return 0
      fi
    done < <(redis_server_candidates_from_root "${deploy_root}")
    echo "could not find redis-server under ${root_env_name}=${deploy_root}; checked src/, bin/, and root" >&2
    return 1
  fi

  if command -v redis-server >/dev/null 2>&1; then
    command -v redis-server
    return 0
  fi

  echo "redis-server not found in PATH; set ${bin_env_name} or ${root_env_name}" >&2
  return 1
}

choose_redis_host_from_conf() {
  local conf=$1
  local host_override=$2
  local bind_host

  if [[ -n "${host_override}" ]]; then
    printf '%s\n' "${host_override}"
    return 0
  fi

  bind_host=$(awk '$1=="bind" { for (i=2; i<=NF; i++) { if ($i !~ /^-/) { print $i; exit } } }' "${conf}")
  if [[ -z "${bind_host}" || "${bind_host}" == "0.0.0.0" || "${bind_host}" == "::" ]]; then
    bind_host="127.0.0.1"
  fi
  printf '%s\n' "${bind_host}"
}

resolve_deploy_addrs() {
  local root_env_name=$1
  local host_env_name=$2
  local deploy_root=${!root_env_name:-}
  local host_override=${!host_env_name:-}
  local conf
  local port
  local host
  local addrs=()

  if [[ -z "${deploy_root}" ]]; then
    return 0
  fi
  if [[ ! -d "${deploy_root}" ]]; then
    echo "${root_env_name} is not a directory: ${deploy_root}" >&2
    return 1
  fi

  while IFS= read -r conf; do
    port=$(awk '$1=="port" { print $2; exit }' "${conf}")
    if [[ -z "${port}" ]]; then
      continue
    fi
    host=$(choose_redis_host_from_conf "${conf}" "${host_override}")
    addrs+=("${host}:${port}")
  done < <(find "${deploy_root}" -type f -name 'redis.conf' | sort)

  if [[ ${#addrs[@]} -eq 0 ]]; then
    echo "could not infer redis addresses from ${root_env_name}=${deploy_root}; no redis.conf with a port was found" >&2
    return 1
  fi

  printf '%s\n' "$(printf '%s\n' "${addrs[@]}" | awk '!seen[$0]++' | paste -sd',' -)"
}

ports_from_addrs() {
  local csv=$1
  local addr
  local cleaned

  IFS=',' read -r -a cleaned <<< "${csv}"
  for addr in "${cleaned[@]}"; do
    addr=${addr##*:}
    if [[ -n "${addr}" ]]; then
      printf '%s\n' "${addr}"
    fi
  done
}

normalize_replay_mode() {
  local mode
  mode=$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')
  case "${mode}" in
    sync)
      printf 'sync\n'
      ;;
    pipeline)
      printf 'pipeline\n'
      ;;
    parallel)
      printf 'parallel\n'
      ;;
    *)
      echo "unknown replay mode: $1 (expected sync, pipeline, or parallel)" >&2
      return 1
      ;;
  esac
}

replay_mode_uses_frontier() {
  local mode
  mode=$(normalize_replay_mode "$1") || return 1
  [[ "${mode}" == "pipeline" || "${mode}" == "parallel" ]]
}

capture_sync_delay_samples() {
  local mode=$1
  local direction=$2
  local port=$3
  local samples_file=$4
  local ts
  ts=$(date -u '+%Y-%m-%dT%H:%M:%SZ')

  curl -sf "http://127.0.0.1:${port}/prometheus" 2>/dev/null | awk \
    -v ts="${ts}" \
    -v mode="${mode}" \
    -v direction="${direction}" \
    -v port="${port}" '
function json_escape(s) {
  gsub(/\\/, "\\\\", s)
  gsub(/"/, "\\\"", s)
  return s
}
$1 ~ /^redisGunYu_output_sync_delay(\{|$)/ {
  series = ""
  if (match($0, /input="[^"]+"/)) {
    series = substr($0, RSTART + 7, RLENGTH - 8)
  }
  value = $NF + 0
  if (value > 0) {
    printf "{\"ts\":\"%s\",\"mode\":\"%s\",\"direction\":\"%s\",\"http_port\":%s,\"input\":\"%s\",\"delay_ns\":%.0f}\n", ts, mode, direction, port, json_escape(series), value
  }
}' >> "${samples_file}" || true
}

monitor_sync_delay_loop() {
  local mode=$1
  local fwd_http_port=$2
  local rev_http_port=$3
  local samples_file=$4
  local interval_seconds=$5

  while true; do
    capture_sync_delay_samples "${mode}" "left_to_right" "${fwd_http_port}" "${samples_file}"
    capture_sync_delay_samples "${mode}" "right_to_left" "${rev_http_port}" "${samples_file}"
    sleep "${interval_seconds}"
  done
}

sync_delay_report_table() {
  local samples_file=$1
  python3 - "${samples_file}" <<'PY'
import json
import math
import sys

path = sys.argv[1]
groups = {}
try:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            value = float(item.get("delay_ns", 0)) / 1_000_000
            if value <= 0 or math.isnan(value) or math.isinf(value):
                continue
            groups.setdefault(item.get("direction", "unknown"), []).append(value)
except FileNotFoundError:
    pass

def percentile(values, pct):
    if not values:
        return 0.0
    values = sorted(values)
    rank = (len(values) - 1) * pct / 100.0
    lower = int(math.floor(rank))
    upper = int(math.ceil(rank))
    if lower == upper:
        return values[lower]
    return values[lower] + (values[upper] - values[lower]) * (rank - lower)

print("| Direction | Samples | p50_ms | p95_ms | p99_ms | max_ms |")
print("| --- | ---: | ---: | ---: | ---: | ---: |")
for direction in ("left_to_right", "right_to_left"):
    values = groups.get(direction, [])
    print(
        f"| {direction} | {len(values)} | "
        f"{percentile(values, 50):.2f} | "
        f"{percentile(values, 95):.2f} | "
        f"{percentile(values, 99):.2f} | "
        f"{(max(values) if values else 0.0):.2f} |"
    )
PY
}

sync_delay_threshold_status() {
  local samples_file=$1
  local max_ms=$2
  python3 - "${samples_file}" "${max_ms}" <<'PY'
import json
import math
import sys

path = sys.argv[1]
threshold = float(sys.argv[2])
if threshold <= 0:
    print("SKIP")
    sys.exit(0)

max_seen = 0.0
count = 0
try:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            value = float(item.get("delay_ns", 0)) / 1_000_000
            if value > 0 and not math.isnan(value) and not math.isinf(value):
                count += 1
                max_seen = max(max_seen, value)
except FileNotFoundError:
    pass

if count == 0:
    print("FAIL no latency samples")
elif max_seen > threshold:
    print(f"FAIL max {max_seen:.2f}ms > {threshold:.2f}ms")
else:
    print(f"PASS max {max_seen:.2f}ms <= {threshold:.2f}ms")
PY
}
