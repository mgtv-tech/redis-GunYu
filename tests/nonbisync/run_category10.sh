#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_ROOT="${TMPDIR:-/tmp}/redisgunyu-nonbisync-cat10"
source "${ROOT}/tests/nonbisync/lib/test_env.sh"
require_test_commands go redis-server redis-cli curl

SCENARIOS="${SCENARIOS:-sync,pipeline}"
SYNC_SRC_BASE="${SYNC_SRC_BASE:-35500}"
SYNC_DST_PORT="${SYNC_DST_PORT:-35600}"
SYNC_HTTP_A="${SYNC_HTTP_A:-35580}"
SYNC_HTTP_B="${SYNC_HTTP_B:-35680}"
PIPE_SRC_BASE="${PIPE_SRC_BASE:-35700}"
PIPE_DST_PORT="${PIPE_DST_PORT:-35800}"
PIPE_HTTP_A="${PIPE_HTTP_A:-35780}"
PIPE_HTTP_B="${PIPE_HTTP_B:-35880}"
TEST_PREFIX="${TEST_PREFIX:-nonbisync:cat10:$(date +%s)}"
SYNCER_PID_A=""
SYNCER_PID_B=""
REDIS_PID_RECORDS=()
REDIS_SERVER_BIN="$(resolve_redis_server_bin REDIS_SERVER_BIN REDIS_DEPLOY_ROOT)"

record_started_redis() {
  local port=$1
  local pid_file=$2
  local pid started
  for _ in $(seq 1 20); do
    if [[ -s "${pid_file}" ]]; then
      pid=$(tr -d '[:space:]' < "${pid_file}")
      if [[ "${pid}" =~ ^[0-9]+$ ]] && kill -0 "${pid}" >/dev/null 2>&1; then
        started=$(ps -p "${pid}" -o lstart= 2>/dev/null | sed 's/^[[:space:]]*//' || true)
        [[ -n "${started}" ]] || break
        REDIS_PID_RECORDS+=("${port}|${pid_file}|${pid}|${started}")
        return 0
      fi
    fi
    sleep 0.1
  done
  echo "could not record Redis process created for port ${port}" >&2
  return 1
}

stop_registered_redis() {
  local record port pid_file pid started actual_pid current_pid current_started
  for record in "${REDIS_PID_RECORDS[@]-}"; do
    [[ -n "${record}" ]] || continue
    IFS='|' read -r port pid_file pid started <<< "${record}"
    [[ -f "${pid_file}" ]] || continue
    current_pid=$(tr -d '[:space:]' < "${pid_file}")
    [[ "${current_pid}" == "${pid}" ]] || continue
    current_started=$(ps -p "${pid}" -o lstart= 2>/dev/null | sed 's/^[[:space:]]*//' || true)
    [[ "${current_started}" == "${started}" ]] || continue
    actual_pid=$(redis-cli -p "${port}" --raw info server 2>/dev/null | awk -F: '$1=="process_id" {gsub("\r", "", $2); print $2; exit}' || true)
    if [[ "${actual_pid}" == "${pid}" ]]; then
      redis-cli -p "${port}" shutdown nosave >/dev/null 2>&1 || true
    elif [[ "$(ps -p "${pid}" -o comm= 2>/dev/null || true)" == *redis-server* ]]; then
      kill "${pid}" >/dev/null 2>&1 || true
    fi
  done
}

port_is_open() {
  local port=$1
  (echo >/dev/tcp/127.0.0.1/"${port}") >/dev/null 2>&1
}

assert_ports_available() {
  local port seen=" "
  for port in "$@"; do
    if [[ ! "${port}" =~ ^[0-9]+$ ]] || ((port < 1 || port > 65535)); then
      echo "invalid test port: ${port}" >&2
      return 1
    fi
    if [[ "${seen}" == *" ${port} "* ]]; then
      echo "duplicate test port: ${port}" >&2
      return 1
    fi
    if port_is_open "${port}"; then
      echo "test port is occupied: ${port}" >&2
      return 1
    fi
    seen+="${port} "
  done
}

start_tracked_cluster_with_replicas() {
  local tmp_root=$1
  local prefix=$2
  shift 2
  local ports=("$@")
  local port dir

  for port in "${ports[@]}"; do
    dir="${tmp_root}/${prefix}-${port}"
    write_cluster_conf "${dir}" "${port}"
    "${REDIS_SERVER_BIN}" "${dir}/redis.conf"
    record_started_redis "${port}" "${dir}/redis.pid"
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

start_tracked_standalone() {
  local tmp_root=$1
  local prefix=$2
  local port=$3
  local dir="${tmp_root}/${prefix}-${port}"
  write_standalone_conf "${dir}" "${port}"
  "${REDIS_SERVER_BIN}" "${dir}/redis.conf"
  record_started_redis "${port}" "${dir}/redis.pid"
  wait_for_ping "${port}"
}

cleanup() {
  local code=$?
  set +e
  stop_pid "${SYNCER_PID_A:-}"
  stop_pid "${SYNCER_PID_B:-}"
  stop_registered_redis
  if [[ "${KEEP_TMP:-0}" != "1" ]]; then
    rm -rf "${TMP_ROOT}"
  fi
  exit "${code}"
}
trap cleanup EXIT

rm -rf "${TMP_ROOT}"
mkdir -p "${TMP_ROOT}"

key_name() {
  local prefix=$1
  local name=$2
  printf "%s:%s" "${prefix}" "${name}"
}

write_phase() {
  local port=$1
  local prefix=$2
  local phase=$3
  case "${phase}" in
    1)
      redis_cmd cluster "${port}" set "$(key_name "${prefix}" "string")" "p1"
      redis_cmd cluster "${port}" incrby "$(key_name "${prefix}" "counter")" 3
      ;;
    2)
      redis_cmd cluster "${port}" set "$(key_name "${prefix}" "string")" "p2"
      redis_cmd cluster "${port}" incrby "$(key_name "${prefix}" "counter")" 4
      redis_cmd cluster "${port}" hset "$(key_name "${prefix}" "hash")" f1 v1 f2 v2
      ;;
  esac
}

write_ha_conf() {
  local file=$1
  local http_port=$2
  local peer_port=$3
  local src_addrs=$4
  local dst_port=$5
  local storer_dir=$6
  local replay_mode=$7
  local group_name=$8

  mkdir -p "${storer_dir}"
  cat > "${file}" <<EOF
server:
  listen: 127.0.0.1:${http_port}
  listenPeer: 127.0.0.1:${peer_port}
  gracefullStopTimeout: 1s
  checkRedisTypologyTicker: 2s
  initialPaused: true
input:
  redis:
    addresses: [${src_addrs}]
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
    addresses: ["127.0.0.1:${dst_port}"]
    type: standalone
    version: "7.0.11"
  replay:
    resumeFromBreakPoint: true
    keyExists: replace
    metric: false
    targetDb: -1
    replayTransaction: true
    bisyncEnabled: false
    mode: ${replay_mode}
cluster:
  groupName: ${group_name}
  leaseTimeout: 6s
log:
  level: info
  handler:
    stdout: true
  withCaller: false
  withFunc: false
EOF
}

wait_for_pipeline_state() {
  local http_port=$1
  local expected_role=$2
  local expected_state=$3
  local expected_count=$4
  local status total role_count state_count
  for _ in $(seq 1 100); do
    status=$(curl -sf "http://127.0.0.1:${http_port}/syncer/status" 2>/dev/null || true)
    total=$(grep -o '"Input"' <<< "${status}" | wc -l | tr -d ' ' || true)
    role_count=$(grep -o "\"Role\":\"${expected_role}\"" <<< "${status}" | wc -l | tr -d ' ' || true)
    state_count=$(grep -o "\"State\":\"${expected_state}\"" <<< "${status}" | wc -l | tr -d ' ' || true)
    if [[ "${total}" == "${expected_count}" && "${role_count}" == "${expected_count}" && "${state_count}" == "${expected_count}" ]]; then
      return 0
    fi
    sleep 0.2
  done
  echo "syncer on port ${http_port} did not expose ${expected_count} ${expected_role}/${expected_state} pipelines" >&2
  curl -s "http://127.0.0.1:${http_port}/syncer/status" >&2 || true
  return 1
}

assert_source_replica_baseline() {
  local expected=$1
  shift
  local port role connected masters=0
  for port in "$@"; do
    role=$(redis-cli -p "${port}" --raw role 2>/dev/null | head -n 1 || true)
    [[ "${role}" == "master" ]] || continue
    masters=$((masters + 1))
    connected=$(redis-cli -p "${port}" --raw info replication 2>/dev/null | awk -F: '$1=="connected_slaves" {gsub("\r", "", $2); print $2; exit}')
    expect_eq "${connected}" "${expected}" "source master ${port} replica clients while initially paused"
  done
  expect_eq "${masters}" "3" "source master count while initially paused"
}

assert_expected_state() {
  local port=$1
  local prefix=$2
  expect_eq "$(redis_call standalone "${port}" get "$(key_name "${prefix}" "string")")" "p2" "$(key_name "${prefix}" "string")"
  expect_eq "$(redis_call standalone "${port}" get "$(key_name "${prefix}" "counter")")" "7" "$(key_name "${prefix}" "counter")"
  expect_eq "$(hash_state standalone "${port}" "$(key_name "${prefix}" "hash")")" "f1=v1|f2=v2" "$(key_name "${prefix}" "hash")"
}

wait_for_phase1_state() {
  local port=$1
  local prefix=$2
  for _ in $(seq 1 80); do
    if [[ "$(redis_call standalone "${port}" get "$(key_name "${prefix}" "string")" 2>/dev/null || true)" == "p1" ]] && \
       [[ "$(redis_call standalone "${port}" get "$(key_name "${prefix}" "counter")" 2>/dev/null || true)" == "3" ]]; then
      return 0
    fi
    sleep 0.5
  done
  echo "standalone target did not reach phase1 state for ${prefix}" >&2
  return 1
}

wait_for_expected_state() {
  local port=$1
  local prefix=$2
  for _ in $(seq 1 80); do
    if [[ "$(redis_call standalone "${port}" get "$(key_name "${prefix}" "string")" 2>/dev/null || true)" == "p2" ]] && \
       [[ "$(redis_call standalone "${port}" get "$(key_name "${prefix}" "counter")" 2>/dev/null || true)" == "7" ]] && \
       [[ "$(hash_state standalone "${port}" "$(key_name "${prefix}" "hash")" 2>/dev/null || true)" == "f1=v1|f2=v2" ]]; then
      return 0
    fi
    sleep 0.5
  done
  echo "standalone target did not reach final expected state for ${prefix}" >&2
  return 1
}

run_scenario() {
  local name=$1
  local mode=$2
  local src_base dst_port http_a http_b
  local src_ports src_csv prefix conf_a conf_b group_name source_master

  if [[ "${mode}" == "sync" ]]; then
    src_base=${SYNC_SRC_BASE}
    dst_port=${SYNC_DST_PORT}
    http_a=${SYNC_HTTP_A}
    http_b=${SYNC_HTTP_B}
  else
    src_base=${PIPE_SRC_BASE}
    dst_port=${PIPE_DST_PORT}
    http_a=${PIPE_HTTP_A}
    http_b=${PIPE_HTTP_B}
  fi

  src_ports=("${src_base}" "$((src_base + 1))" "$((src_base + 2))" "$((src_base + 3))" "$((src_base + 4))" "$((src_base + 5))")
  src_csv=$(format_addrs "${src_ports[@]}")
  prefix="${TEST_PREFIX}:${name}"
  group_name="nonbisync-cat10-${name}"

  echo "[category10] scenario=${name} mode=${mode}"
  assert_ports_available "${src_ports[@]}" "${dst_port}" "${http_a}" "${http_b}"
  start_tracked_cluster_with_replicas "${TMP_ROOT}" "${name}-src" "${src_ports[@]}"
  start_tracked_standalone "${TMP_ROOT}" "${name}-dst" "${dst_port}"

  conf_a="${TMP_ROOT}/${name}-a.yaml"
  conf_b="${TMP_ROOT}/${name}-b.yaml"
  write_ha_conf "${conf_a}" "${http_a}" "${http_a}" "${src_csv}" "${dst_port}" "${TMP_ROOT}/${name}-a-store" "${mode}" "${group_name}"
  write_ha_conf "${conf_b}" "${http_b}" "${http_b}" "${src_csv}" "${dst_port}" "${TMP_ROOT}/${name}-b-store" "${mode}" "${group_name}"

  SYNCER_PID_A=$(start_syncer_process "${TMP_ROOT}" "${conf_a}" "${TMP_ROOT}/${name}-a.log")
  wait_for_syncer "${http_a}"
  wait_for_log_pattern "${TMP_ROOT}/${name}-a.log" 'new_role\(leader\)|RunLeader' 20
  wait_for_pipeline_state "${http_a}" leader pause 3

  SYNCER_PID_B=$(start_syncer_process "${TMP_ROOT}" "${conf_b}" "${TMP_ROOT}/${name}-b.log")
  wait_for_syncer "${http_b}"
  wait_for_log_pattern "${TMP_ROOT}/${name}-b.log" 'new_role\(follower\)|RunFollower' 20
  wait_for_pipeline_state "${http_b}" follower pause 3

  source_master=$(find_first_master_port "${src_ports[@]}")
  write_phase "${source_master}" "${prefix}" 1
  sleep 1
  expect_eq "$(scan_count_standalone "${dst_port}" "${prefix}:*")" "0" "business data before HA resume"
  assert_source_replica_baseline 1 "${src_ports[@]}"

  curl -sf -XPOST "http://127.0.0.1:${http_a}/syncer/resume?inputs=all" >/dev/null
  curl -sf -XPOST "http://127.0.0.1:${http_b}/syncer/resume?inputs=all" >/dev/null
  wait_for_pipeline_state "${http_a}" leader run 3
  wait_for_pipeline_state "${http_b}" follower run 3
  wait_for_phase1_state "${dst_port}" "${prefix}"

  stop_pid "${SYNCER_PID_A}"
  SYNCER_PID_A=""
  wait_for_log_pattern "${TMP_ROOT}/${name}-b.log" 'new_role\(leader\)|RunLeader' 20
  wait_for_pipeline_state "${http_b}" leader run 3

  source_master=$(find_first_master_port "${src_ports[@]}")
  write_phase "${source_master}" "${prefix}" 2
  write_bulk_dataset cluster "${source_master}" "${prefix}" "leader-handover"
  redis_cmd standalone "${dst_port}" set "nonbisync:cat10:isolated:${name}" "target-only"
  wait_for_redis_equal "${TMP_ROOT}" "${src_csv}" cluster 0 "127.0.0.1:${dst_port}" standalone 0 "${prefix}:*"
  wait_for_expected_state "${dst_port}" "${prefix}"

  assert_expected_state "${dst_port}" "${prefix}"
  assert_min_key_count_standalone "${dst_port}" "${prefix}:*" "$(bulk_dataset_min_keys)"
  expect_absent "$(redis_call cluster "${source_master}" exists "nonbisync:cat10:isolated:${name}")" "isolated target key on source"
  assert_no_bisync_metadata_standalone "${dst_port}"
  assert_checkpoint_signals_standalone "${dst_port}"
  assert_log_has_no_bisync_markers "${TMP_ROOT}/${name}-a.log"
  assert_log_has_no_bisync_markers "${TMP_ROOT}/${name}-b.log"

  stop_pid "${SYNCER_PID_A:-}"
  stop_pid "${SYNCER_PID_B:-}"
  SYNCER_PID_A=""
  SYNCER_PID_B=""
  stop_registered_redis
  REDIS_PID_RECORDS=()
}

echo "[1/1] building binaries"
build_nonbisync_binaries "${TMP_ROOT}"

case ",${SCENARIOS}," in
  *",sync,"*)
    run_scenario sync sync
    ;;
esac
case ",${SCENARIOS}," in
  *",pipeline,"*)
    run_scenario pipeline pipeline
    ;;
esac
