#!/usr/bin/env bash

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
source "${ROOT}/tests/bisync/lib/redis_env.sh"

stop_pid() {
  local pid=${1:-}
  if [[ -n "${pid}" ]]; then
    kill "${pid}" >/dev/null 2>&1 || true
    wait "${pid}" >/dev/null 2>&1 || true
  fi
}

shutdown_ports() {
  local port
  for port in "$@"; do
    redis-cli -p "${port}" shutdown nosave >/dev/null 2>&1 || true
  done
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

write_cluster_conf() {
  local dir=$1
  local port=$2
  local extra_conf=${3:-}
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
  if [[ -n "${extra_conf}" ]]; then
    printf '%s\n' "${extra_conf}" >> "${dir}/redis.conf"
  fi
}

write_standalone_conf() {
  local dir=$1
  local port=$2
  local extra_conf=${3:-}
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
EOF
  if [[ -n "${extra_conf}" ]]; then
    printf '%s\n' "${extra_conf}" >> "${dir}/redis.conf"
  fi
}

start_cluster() {
  local redis_server_bin=$1
  local tmp_root=$2
  local prefix=$3
  shift 3
  local ports=("$@")
  local port

  for port in "${ports[@]}"; do
    write_cluster_conf "${tmp_root}/${prefix}-${port}" "${port}"
    "${redis_server_bin}" "${tmp_root}/${prefix}-${port}/redis.conf"
    wait_for_ping "${port}"
  done

  redis-cli --cluster create \
    $(printf '127.0.0.1:%s ' "${ports[@]}") \
    --cluster-replicas 0 \
    --cluster-yes >/dev/null

  for port in "${ports[@]}"; do
    wait_for_cluster_ok "${port}"
  done
  sleep 1
}

start_cluster_with_replicas() {
  local redis_server_bin=$1
  local tmp_root=$2
  local prefix=$3
  shift 3
  local ports=("$@")
  local port

  for port in "${ports[@]}"; do
    write_cluster_conf "${tmp_root}/${prefix}-${port}" "${port}"
    "${redis_server_bin}" "${tmp_root}/${prefix}-${port}/redis.conf"
    wait_for_ping "${port}"
  done

  redis-cli --cluster create \
    $(printf '127.0.0.1:%s ' "${ports[@]}") \
    --cluster-replicas 1 \
    --cluster-yes >/dev/null

  for port in "${ports[@]}"; do
    wait_for_cluster_ok "${port}"
  done
  sleep 1
}

start_standalone() {
  local redis_server_bin=$1
  local tmp_root=$2
  local prefix=$3
  local port=$4
  local extra_conf=${5:-}

  write_standalone_conf "${tmp_root}/${prefix}-${port}" "${port}" "${extra_conf}"
  "${redis_server_bin}" "${tmp_root}/${prefix}-${port}/redis.conf"
  wait_for_ping "${port}"
}

build_nonbisync_binaries() {
  local tmp_root=$1
  mkdir -p "${tmp_root}/gocache"
  (cd "${ROOT}" && GOCACHE="${tmp_root}/gocache" go build -o "${tmp_root}/redisGunYu" ./main.go)
  (cd "${ROOT}" && GOCACHE="${tmp_root}/gocache" go build -o "${tmp_root}/bisync_compare" ./tests/bisync/cmd/bisync_compare)
  (cd "${ROOT}" && GOCACHE="${tmp_root}/gocache" go build -o "${tmp_root}/nonbisync_workload" ./tests/nonbisync/cmd/nonbisync_workload)
}

format_addrs() {
  local port
  local first=1
  for port in "$@"; do
    if [[ ${first} -eq 1 ]]; then
      printf '127.0.0.1:%s' "${port}"
      first=0
    else
      printf ',127.0.0.1:%s' "${port}"
    fi
  done
  printf '\n'
}

write_syncer_conf() {
  local file=$1
  local http_port=$2
  local input_addrs=$3
  local input_type=$4
  local output_addrs=$5
  local output_type=$6
  local storer_dir=$7
  local replay_mode=$8

  mkdir -p "${storer_dir}"
  cat > "${file}" <<EOF
server:
  listen: 127.0.0.1:${http_port}
  listenPeer: 127.0.0.1:${http_port}
  gracefullStopTimeout: 1s
  checkRedisTypologyTicker: 2s
input:
  redis:
    addresses: [${input_addrs}]
    type: ${input_type}
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
    type: ${output_type}
    version: "7.0.11"
  replay:
    resumeFromBreakPoint: true
    keyExists: replace
    metric: false
    targetDb: -1
    replayTransaction: true
    bisyncEnabled: false
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

start_syncer_process() {
  local tmp_root=$1
  local conf_file=$2
  local log_file=$3
  "${tmp_root}/redisGunYu" -conf "${conf_file}" -cmd sync >"${log_file}" 2>&1 &
  echo $!
}

restart_syncer_via_api() {
  local port=$1
  curl -sf -XPOST "http://127.0.0.1:${port}/syncer/restart" >/dev/null
  wait_for_syncer "${port}"
  sleep 2
}

redis_call() {
  local mode=$1
  local port=$2
  shift 2
  if [[ "${mode}" == "cluster" ]]; then
    redis-cli -c -p "${port}" --raw "$@"
  else
    redis-cli -p "${port}" --raw "$@"
  fi
}

redis_call_db() {
  local mode=$1
  local port=$2
  local db=$3
  shift 3
  if [[ "${mode}" == "cluster" ]]; then
    redis-cli -c -p "${port}" --raw "$@"
  else
    redis-cli -n "${db}" -p "${port}" --raw "$@"
  fi
}

redis_cmd() {
  local mode=$1
  local port=$2
  shift 2
  redis_call "${mode}" "${port}" "$@" >/dev/null
}

redis_cmd_db() {
  local mode=$1
  local port=$2
  local db=$3
  shift 3
  redis_call_db "${mode}" "${port}" "${db}" "$@" >/dev/null
}

bulk_dataset_key_count() {
  echo "${NONBISYNC_BULK_KEY_COUNT:-128}"
}

bulk_dataset_min_keys() {
  local count=${1:-$(bulk_dataset_key_count)}
  echo $((count * 6))
}

write_bulk_dataset() {
  local mode=$1
  local port=$2
  local prefix=$3
  local phase=$4
  local count=${5:-$(bulk_dataset_key_count)}
  local i suffix base

  for i in $(seq 1 "${count}"); do
    printf -v suffix "%04d" "${i}"
    base="${prefix}:bulk:${phase}:${suffix}"
    redis_cmd "${mode}" "${port}" set "${base}:string" "value-${phase}-${suffix}"
    redis_cmd "${mode}" "${port}" incrby "${base}:counter" "$(((i % 17) + 1))"
    redis_cmd "${mode}" "${port}" hset "${base}:hash" idx "${suffix}" phase "${phase}" state active
    redis_cmd "${mode}" "${port}" rpush "${base}:list" "a-${suffix}" "b-${suffix}" "c-${suffix}"
    redis_cmd "${mode}" "${port}" sadd "${base}:set" "red-${suffix}" "blue-${suffix}" "green-${suffix}"
    redis_cmd "${mode}" "${port}" zadd "${base}:zset" "${i}" "member-${suffix}"
  done
}

scan_keys() {
  local port=$1
  local pattern=$2
  redis-cli -p "${port}" --scan --pattern "${pattern}" 2>/dev/null | sort
}

scan_keys_db() {
  local port=$1
  local db=$2
  local pattern=$3
  redis-cli -n "${db}" -p "${port}" --scan --pattern "${pattern}" 2>/dev/null | sort
}

trim_joined_lines() {
  tr '\n' ' ' | sed 's/[[:space:]]*$//'
}

set_state() {
  local mode=$1
  local port=$2
  local key=$3
  redis_call "${mode}" "${port}" smembers "${key}" | sort | trim_joined_lines
}

list_state() {
  local mode=$1
  local port=$2
  local key=$3
  redis_call "${mode}" "${port}" lrange "${key}" 0 -1 | trim_joined_lines
}

hash_state() {
  local mode=$1
  local port=$2
  local key=$3
  redis_call "${mode}" "${port}" hgetall "${key}" | awk 'NR % 2 == 1 { printf "%s=", $0; next } { printf "%s\n", $0 }' | sort | paste -sd'|' -
}

zset_state() {
  local mode=$1
  local port=$2
  local key=$3
  redis_call "${mode}" "${port}" zrange "${key}" 0 -1 withscores | awk 'NR % 2 == 1 { printf "%s=", $0; next } { printf "%s\n", $0 }' | paste -sd'|' -
}

dump_key_state() {
  local port=$1
  local key=$2
  dump_key_state_by_mode standalone "${port}" "${key}"
}

dump_key_state_by_mode() {
  local mode=$1
  local port=$2
  local key=$3
  local type
  type=$(redis_call "${mode}" "${port}" type "${key}")
  case "${type}" in
    string)
      printf '%s|string|%s\n' "${key}" "$(redis_call "${mode}" "${port}" get "${key}")"
      ;;
    set)
      printf '%s|set|%s\n' "${key}" "$(set_state "${mode}" "${port}" "${key}")"
      ;;
    hash)
      printf '%s|hash|%s\n' "${key}" "$(hash_state "${mode}" "${port}" "${key}")"
      ;;
    list)
      printf '%s|list|%s\n' "${key}" "$(list_state "${mode}" "${port}" "${key}")"
      ;;
    zset)
      printf '%s|zset|%s\n' "${key}" "$(zset_state "${mode}" "${port}" "${key}")"
      ;;
    none)
      ;;
    *)
      printf '%s|type:%s|\n' "${key}" "${type}"
      ;;
  esac
}

dump_cluster_key_state() {
  local port=$1
  local key=$2
  dump_key_state_by_mode cluster "${port}" "${key}"
}

cluster_key_slot() {
  local port=$1
  local key=$2
  redis-cli -c -p "${port}" --raw cluster keyslot "${key}" 2>/dev/null || echo unknown
}

cluster_port_snapshot() {
  local port=$1
  local role state myself
  role=$(redis-cli -p "${port}" --raw role 2>/dev/null | head -n 1 || true)
  state=$(redis-cli -p "${port}" cluster info 2>/dev/null | awk -F: '$1=="cluster_state" {print $2; exit}' | tr -d '\r' || true)
  myself=$(redis-cli -p "${port}" cluster nodes 2>/dev/null | awk '$3 ~ /myself/ {print $0; exit}' || true)
  printf 'port=%s role=%s cluster_state=%s myself=%s\n' "${port}" "${role:-unknown}" "${state:-unknown}" "${myself:-unknown}"
}

dump_standalone_state() {
  local port=$1
  local pattern=$2
  local key
  while IFS= read -r key; do
    [[ -n "${key}" ]] || continue
    dump_key_state "${port}" "${key}"
  done < <(scan_keys "${port}" "${pattern}")
}

redis_compare() {
  local tmp_root=$1
  local left_addrs=$2
  local left_type=$3
  local left_db=$4
  local right_addrs=$5
  local right_type=$6
  local right_db=$7
  local pattern=$8
  "${tmp_root}/bisync_compare" \
    --left-addrs "${left_addrs}" \
    --left-type "${left_type}" \
    --left-db "${left_db}" \
    --right-addrs "${right_addrs}" \
    --right-type "${right_type}" \
    --right-db "${right_db}" \
    --pattern "${pattern}"
}

wait_for_redis_equal() {
  local tmp_root=$1
  local left_addrs=$2
  local left_type=$3
  local left_db=$4
  local right_addrs=$5
  local right_type=$6
  local right_db=$7
  local pattern=$8

  for _ in $(seq 1 80); do
    if redis_compare "${tmp_root}" "${left_addrs}" "${left_type}" "${left_db}" "${right_addrs}" "${right_type}" "${right_db}" "${pattern}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done

  redis_compare "${tmp_root}" "${left_addrs}" "${left_type}" "${left_db}" "${right_addrs}" "${right_type}" "${right_db}" "${pattern}" || true
  echo "redis instances did not converge for pattern ${pattern}: left=${left_type}:${left_addrs}/db${left_db} right=${right_type}:${right_addrs}/db${right_db}" >&2
  return 1
}

wait_for_standalone_equal() {
  local left_port=$1
  local right_port=$2
  local pattern=$3
  local tmp_root=${4:-}
  if [[ -z "${tmp_root}" ]]; then
    for _ in $(seq 1 80); do
      if diff -u <(dump_standalone_state "${left_port}" "${pattern}") <(dump_standalone_state "${right_port}" "${pattern}") >/dev/null 2>&1; then
        return 0
      fi
      sleep 0.5
    done

    diff -u <(dump_standalone_state "${left_port}" "${pattern}") <(dump_standalone_state "${right_port}" "${pattern}") || true
    echo "standalone redis instances did not converge for pattern ${pattern}" >&2
    return 1
  fi
  wait_for_redis_equal "${tmp_root}" "127.0.0.1:${left_port}" standalone 0 "127.0.0.1:${right_port}" standalone 0 "${pattern}"
}

cluster_compare() {
  local tmp_root=$1
  local left_addrs=$2
  local right_addrs=$3
  local pattern=$4
  redis_compare "${tmp_root}" "${left_addrs}" cluster 0 "${right_addrs}" cluster 0 "${pattern}"
}

wait_for_cluster_equal() {
  local tmp_root=$1
  local left_addrs=$2
  local right_addrs=$3
  local pattern=$4

  for _ in $(seq 1 80); do
    if cluster_compare "${tmp_root}" "${left_addrs}" "${right_addrs}" "${pattern}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done

  cluster_compare "${tmp_root}" "${left_addrs}" "${right_addrs}" "${pattern}" || true
  echo "clusters did not converge for pattern ${pattern}" >&2
  return 1
}

wait_for_absent_db() {
  local mode=$1
  local port=$2
  local db=$3
  local key=$4
  local label=$5
  for _ in $(seq 1 80); do
    if [[ "$(redis_call_db "${mode}" "${port}" "${db}" exists "${key}" 2>/dev/null || echo 0)" == "0" ]]; then
      return 0
    fi
    sleep 0.25
  done
  echo "expected ${label} to stay absent" >&2
  return 1
}

scan_count_cluster() {
  local pattern=$1
  shift
  local port
  for port in "$@"; do
    redis-cli -p "${port}" --scan --pattern "${pattern}" 2>/dev/null || true
  done | sort -u | sed '/^$/d' | wc -l | tr -d ' '
}

scan_count_standalone() {
  local port=$1
  local pattern=$2
  scan_keys "${port}" "${pattern}" | wc -l | tr -d ' '
}

scan_count_standalone_db() {
  local port=$1
  local db=$2
  local pattern=$3
  scan_keys_db "${port}" "${db}" "${pattern}" | wc -l | tr -d ' '
}

assert_min_key_count_cluster() {
  local pattern=$1
  local min_count=$2
  shift 2
  local count
  count=$(scan_count_cluster "${pattern}" "$@")
  if (( count < min_count )); then
    echo "expected at least ${min_count} cluster keys for pattern ${pattern}, got ${count}" >&2
    exit 1
  fi
}

assert_min_key_count_standalone() {
  local port=$1
  local pattern=$2
  local min_count=$3
  local count
  count=$(scan_count_standalone "${port}" "${pattern}")
  if (( count < min_count )); then
    echo "expected at least ${min_count} standalone keys for pattern ${pattern}, got ${count}" >&2
    exit 1
  fi
}

assert_no_bisync_metadata_cluster() {
  local marker_count latest_count commit_count frontier_count
  marker_count=$(scan_count_cluster 'redis-gunyu-bisync:*:marker:*' "$@")
  latest_count=$(scan_count_cluster 'redis-gunyu-bisync:*:latest:*' "$@")
  commit_count=$(scan_count_cluster 'redis-gunyu-bisync:*:commit:*' "$@")
  frontier_count=$(scan_count_cluster 'redis-gunyu-bisync:*:frontier' "$@")
  if [[ "${marker_count}" != "0" || "${latest_count}" != "0" || "${commit_count}" != "0" || "${frontier_count}" != "0" ]]; then
    echo "expected no bisync metadata, got marker=${marker_count} latest=${latest_count} commit=${commit_count} frontier=${frontier_count}" >&2
    exit 1
  fi
}

assert_no_bisync_metadata_standalone() {
  local port=$1
  local marker_count latest_count commit_count frontier_count
  marker_count=$(scan_count_standalone "${port}" 'redis-gunyu-bisync:*:marker:*')
  latest_count=$(scan_count_standalone "${port}" 'redis-gunyu-bisync:*:latest:*')
  commit_count=$(scan_count_standalone "${port}" 'redis-gunyu-bisync:*:commit:*')
  frontier_count=$(scan_count_standalone "${port}" 'redis-gunyu-bisync:*:frontier')
  if [[ "${marker_count}" != "0" || "${latest_count}" != "0" || "${commit_count}" != "0" || "${frontier_count}" != "0" ]]; then
    echo "expected no bisync metadata, got marker=${marker_count} latest=${latest_count} commit=${commit_count} frontier=${frontier_count}" >&2
    exit 1
  fi
}

assert_checkpoint_signals_cluster() {
  local port=$1
  shift
  local hash_len key_count
  hash_len=$(redis-cli -c -p "${port}" --raw hlen redis-gunyu-checkpoint-hash 2>/dev/null || echo 0)
  key_count=$(scan_count_cluster 'redis-gunyu-checkpoint*' "${port}" "$@")
  if [[ "${hash_len}" == "0" || "${key_count}" == "0" ]]; then
    echo "expected checkpoint signals on cluster target, got hash_len=${hash_len} key_count=${key_count}" >&2
    exit 1
  fi
}

assert_checkpoint_signals_standalone() {
  local port=$1
  local hash_len key_count
  hash_len=$(redis-cli -p "${port}" --raw hlen redis-gunyu-checkpoint-hash 2>/dev/null || echo 0)
  key_count=$(scan_count_standalone "${port}" 'redis-gunyu-checkpoint*')
  if [[ "${hash_len}" == "0" || "${key_count}" == "0" ]]; then
    echo "expected checkpoint signals on standalone target, got hash_len=${hash_len} key_count=${key_count}" >&2
    exit 1
  fi
}

assert_log_has_no_bisync_markers() {
  local log_file=$1
  if match_regex_quiet 'bisync startpoint|bisync checkpoint namespace|scheme1|frontier' "${log_file}"; then
    echo "expected non-bisync log without bisync markers: ${log_file}" >&2
    exit 1
  fi
}

assert_log_has_resume_signal() {
  local log_file=$1
  if ! match_regex_quiet 'resume from checkpoint|UpdateCheckpoint|checkpoint|psync : runId\(.*local\(\{0 [^ ]+ [1-9][0-9]*\}\)|psync : runId\(.*output\(\{0 [^ ]+ [1-9][0-9]*\}\)' "${log_file}"; then
    echo "expected checkpoint resume signal in ${log_file}" >&2
    exit 1
  fi
}

assert_log_has_topology_signal() {
  local log_file=$1
  if ! match_regex_quiet 'restart|typology|redis typology is changed|run error|MOVED|ASK|role' "${log_file}"; then
    echo "expected topology or restart signal in ${log_file}" >&2
    exit 1
  fi
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
    echo "assertion failed for ${label}: expected absent, got exists=${actual}" >&2
    exit 1
  fi
}

find_first_master_port() {
  local port
  for port in "$@"; do
    if [[ "$(redis-cli -p "${port}" --raw role 2>/dev/null | head -n 1)" == "master" ]]; then
      echo "${port}"
      return 0
    fi
  done
  return 1
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
  for _ in $(seq 1 80); do
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
  sleep 1
}

wait_for_replica_caught_up() {
  local replica_port=$1
  local state master_port replica_offset master_offset info
  for _ in $(seq 1 120); do
    state=$(redis-cli -p "${replica_port}" --raw role 2>/dev/null | sed -n '4p' || true)
    master_port=$(redis-cli -p "${replica_port}" --raw role 2>/dev/null | sed -n '3p' || true)
    replica_offset=$(redis-cli -p "${replica_port}" --raw role 2>/dev/null | sed -n '5p' || true)
    if [[ -n "${master_port}" && "${state}" == "connected" && "${replica_offset}" =~ ^[0-9]+$ ]]; then
      info=$(redis-cli -p "${master_port}" info replication 2>/dev/null | awk -F: '$1=="master_repl_offset" {gsub("\r","",$2); print $2; exit}' || true)
      if [[ "${info}" =~ ^[0-9]+$ ]] && (( replica_offset >= info )); then
        return 0
      fi
    fi
    sleep 0.25
  done
  echo "replica on port ${replica_port} did not catch up before failover" >&2
  return 1
}

wait_for_pid_exit() {
  local pid=$1
  local timeout_seconds=${2:-20}
  local waited=0
  while kill -0 "${pid}" >/dev/null 2>&1; do
    if (( waited >= timeout_seconds * 4 )); then
      echo "pid ${pid} did not exit within ${timeout_seconds}s" >&2
      return 1
    fi
    sleep 0.25
    waited=$((waited + 1))
  done
}

wait_for_log_pattern() {
  local log_file=$1
  local pattern=$2
  local timeout_seconds=${3:-20}
  local waited=0
  while (( waited < timeout_seconds * 4 )); do
    if [[ -f "${log_file}" ]] && match_regex_quiet "${pattern}" "${log_file}"; then
      return 0
    fi
    sleep 0.25
    waited=$((waited + 1))
  done
  echo "pattern [${pattern}] not found in ${log_file} within ${timeout_seconds}s" >&2
  return 1
}
