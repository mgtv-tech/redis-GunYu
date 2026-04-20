#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_ROOT="${TMPDIR:-/tmp}/redisgunyu-nonbisync-cat7"
source "${ROOT}/tests/nonbisync/lib/test_env.sh"

SCENARIOS="${SCENARIOS:-sync,pipeline}"
SYNC_STD_SRC_PORT="${SYNC_STD_SRC_PORT:-33900}"
SYNC_STD_DST_PORT="${SYNC_STD_DST_PORT:-34000}"
SYNC_STD_HTTP_PORT="${SYNC_STD_HTTP_PORT:-33980}"
SYNC_CLUSTER_SRC_BASE="${SYNC_CLUSTER_SRC_BASE:-34100}"
SYNC_CLUSTER_DST_BASE="${SYNC_CLUSTER_DST_BASE:-34200}"
SYNC_CLUSTER_HTTP_PORT="${SYNC_CLUSTER_HTTP_PORT:-34180}"
PIPE_STD_SRC_PORT="${PIPE_STD_SRC_PORT:-34300}"
PIPE_STD_DST_PORT="${PIPE_STD_DST_PORT:-34400}"
PIPE_STD_HTTP_PORT="${PIPE_STD_HTTP_PORT:-34380}"
PIPE_CLUSTER_SRC_BASE="${PIPE_CLUSTER_SRC_BASE:-34500}"
PIPE_CLUSTER_DST_BASE="${PIPE_CLUSTER_DST_BASE:-34600}"
PIPE_CLUSTER_HTTP_PORT="${PIPE_CLUSTER_HTTP_PORT:-34580}"
TEST_PREFIX="${TEST_PREFIX:-nonbisync:cat7:$(date +%s)}"
SYNCER_PID=""
REDIS_SERVER_BIN="$(resolve_redis_server_bin REDIS_SERVER_BIN REDIS_DEPLOY_ROOT)"

cleanup() {
  local code=$?
  set +e
  stop_pid "${SYNCER_PID:-}"
  shutdown_ports \
    "${SYNC_STD_SRC_PORT}" "${SYNC_STD_DST_PORT}" \
    "${SYNC_CLUSTER_SRC_BASE}" "$((SYNC_CLUSTER_SRC_BASE + 1))" "$((SYNC_CLUSTER_SRC_BASE + 2))" \
    "${SYNC_CLUSTER_DST_BASE}" "$((SYNC_CLUSTER_DST_BASE + 1))" "$((SYNC_CLUSTER_DST_BASE + 2))" \
    "${PIPE_STD_SRC_PORT}" "${PIPE_STD_DST_PORT}" \
    "${PIPE_CLUSTER_SRC_BASE}" "$((PIPE_CLUSTER_SRC_BASE + 1))" "$((PIPE_CLUSTER_SRC_BASE + 2))" \
    "${PIPE_CLUSTER_DST_BASE}" "$((PIPE_CLUSTER_DST_BASE + 1))" "$((PIPE_CLUSTER_DST_BASE + 2))"
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

write_matrix_conf() {
  local file=$1
  local http_port=$2
  local input_addrs=$3
  local input_type=$4
  local output_addrs=$5
  local output_type=$6
  local storer_dir=$7
  local replay_mode=$8
  local resume_from_breakpoint=$9
  local key_exists=${10}
  local replay_extra=${11:-}
  local filter_block=${12:-}

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
    resumeFromBreakPoint: ${resume_from_breakpoint}
    keyExists: ${key_exists}
    metric: false
    targetDb: -1
    replayTransaction: true
    bisyncEnabled: false
    mode: ${replay_mode}
EOF

  if [[ -n "${replay_extra}" ]]; then
    printf '%s\n' "${replay_extra}" >> "${file}"
  fi
  if [[ -n "${filter_block}" ]]; then
    printf '%s\n' "${filter_block}" >> "${file}"
  fi

  cat >> "${file}" <<EOF
log:
  level: info
  handler:
    stdout: true
  withCaller: false
  withFunc: false
EOF
}

wait_for_value() {
  local mode=$1
  local port=$2
  local db=$3
  local key=$4
  local expected=$5
  local label=$6
  for _ in $(seq 1 80); do
    if [[ "$(redis_call_db "${mode}" "${port}" "${db}" get "${key}" 2>/dev/null || true)" == "${expected}" ]]; then
      return 0
    fi
    sleep 0.25
  done
  echo "expected ${label} to become ${expected}" >&2
  return 1
}

run_key_exists_matrix() {
  local name=$1
  local mode=$2
  local src_port dst_port http_port
  local prefix replay_mode conf_file

  if [[ "${mode}" == "sync" ]]; then
    src_port=${SYNC_STD_SRC_PORT}
    dst_port=${SYNC_STD_DST_PORT}
    http_port=${SYNC_STD_HTTP_PORT}
  else
    src_port=${PIPE_STD_SRC_PORT}
    dst_port=${PIPE_STD_DST_PORT}
    http_port=${PIPE_STD_HTTP_PORT}
  fi

  prefix="${TEST_PREFIX}:${name}:keyexists"
  replay_mode="${mode}"
  echo "[category7] scenario=${name} matrix=keyExists mode=${mode}"

  for strategy in replace ignore error; do
    shutdown_ports "${src_port}" "${dst_port}"
    start_standalone "${REDIS_SERVER_BIN}" "${TMP_ROOT}" "${name}-${strategy}-src" "${src_port}"
    start_standalone "${REDIS_SERVER_BIN}" "${TMP_ROOT}" "${name}-${strategy}-dst" "${dst_port}"

    redis_cmd standalone "${src_port}" set "$(key_name "${prefix}" "conflict")" "source-${strategy}"
    redis_cmd standalone "${src_port}" set "$(key_name "${prefix}" "normal")" "normal-${strategy}"
    redis_cmd standalone "${dst_port}" set "$(key_name "${prefix}" "conflict")" "target-${strategy}"

    conf_file="${TMP_ROOT}/${name}-${strategy}.yaml"
    write_matrix_conf "${conf_file}" "${http_port}" "\"127.0.0.1:${src_port}\"" standalone "\"127.0.0.1:${dst_port}\"" standalone "${TMP_ROOT}/${name}-${strategy}-store" "${replay_mode}" false "${strategy}"
    SYNCER_PID=$(start_syncer_process "${TMP_ROOT}" "${conf_file}" "${TMP_ROOT}/${name}-${strategy}.log")

    case "${strategy}" in
      replace)
        wait_for_syncer "${http_port}"
        wait_for_value standalone "${dst_port}" 0 "$(key_name "${prefix}" "conflict")" "source-${strategy}" "replace conflict key"
        wait_for_value standalone "${dst_port}" 0 "$(key_name "${prefix}" "normal")" "normal-${strategy}" "replace normal key"
        assert_no_bisync_metadata_standalone "${dst_port}"
        stop_pid "${SYNCER_PID}"
        SYNCER_PID=""
        ;;
      ignore)
        wait_for_syncer "${http_port}"
        wait_for_value standalone "${dst_port}" 0 "$(key_name "${prefix}" "conflict")" "target-${strategy}" "ignore conflict key"
        wait_for_value standalone "${dst_port}" 0 "$(key_name "${prefix}" "normal")" "normal-${strategy}" "ignore normal key"
        assert_no_bisync_metadata_standalone "${dst_port}"
        stop_pid "${SYNCER_PID}"
        SYNCER_PID=""
        ;;
      error)
        wait_for_log_pattern "${TMP_ROOT}/${name}-${strategy}.log" 'BUSYKEY|restore rdb error|key exist' 20
        wait_for_value standalone "${dst_port}" 0 "$(key_name "${prefix}" "conflict")" "target-${strategy}" "error conflict key"
        wait_for_value standalone "${dst_port}" 0 "$(key_name "${prefix}" "normal")" "normal-${strategy}" "error normal key"
        stop_pid "${SYNCER_PID}"
        SYNCER_PID=""
        ;;
    esac
  done

  shutdown_ports "${src_port}" "${dst_port}"
}

run_filter_and_dbmap_matrix() {
  local name=$1
  local mode=$2
  local src_port dst_port http_port
  local prefix replay_mode conf_file replay_extra filter_block

  if [[ "${mode}" == "sync" ]]; then
    src_port=${SYNC_STD_SRC_PORT}
    dst_port=${SYNC_STD_DST_PORT}
    http_port=${SYNC_STD_HTTP_PORT}
  else
    src_port=${PIPE_STD_SRC_PORT}
    dst_port=${PIPE_STD_DST_PORT}
    http_port=${PIPE_STD_HTTP_PORT}
  fi

  prefix="${TEST_PREFIX}:${name}:filters"
  replay_mode="${mode}"
  echo "[category7] scenario=${name} matrix=filter-dbmap mode=${mode}"

  start_standalone "${REDIS_SERVER_BIN}" "${TMP_ROOT}" "${name}-filters-src" "${src_port}"
  start_standalone "${REDIS_SERVER_BIN}" "${TMP_ROOT}" "${name}-filters-dst" "${dst_port}"

  redis_cmd_db standalone "${src_port}" 0 set "$(key_name "${prefix}" "keep:string")" "keep-db0"
  redis_cmd_db standalone "${src_port}" 0 set "$(key_name "${prefix}" "drop:prefix")" "drop-prefix"
  redis_cmd_db standalone "${src_port}" 1 set "$(key_name "${prefix}" "map:string")" "map-db1"
  redis_cmd_db standalone "${src_port}" 2 set "$(key_name "${prefix}" "drop:db")" "drop-db2"

  replay_extra=$(cat <<EOF
    targetDbMap:
      1: 5
EOF
)
  filter_block=$(cat <<EOF
  filter:
    dbBlacklist: [2]
    keyFilter:
      prefixKeyWhitelist: ["${prefix}:keep", "${prefix}:map"]
      prefixKeyBlacklist: ["${prefix}:drop"]
EOF
)

  conf_file="${TMP_ROOT}/${name}-filters.yaml"
  write_matrix_conf "${conf_file}" "${http_port}" "\"127.0.0.1:${src_port}\"" standalone "\"127.0.0.1:${dst_port}\"" standalone "${TMP_ROOT}/${name}-filters-store" "${replay_mode}" false replace "${replay_extra}" "${filter_block}"
  SYNCER_PID=$(start_syncer_process "${TMP_ROOT}" "${conf_file}" "${TMP_ROOT}/${name}-filters.log")
  wait_for_syncer "${http_port}"

  wait_for_value standalone "${dst_port}" 0 "$(key_name "${prefix}" "keep:string")" "keep-db0" "kept db0 key"
  wait_for_absent_db standalone "${dst_port}" 0 "$(key_name "${prefix}" "drop:prefix")" "blacklisted prefix key"
  wait_for_absent_db standalone "${dst_port}" 0 "$(key_name "${prefix}" "map:string")" "mapped key in db0"
  wait_for_value standalone "${dst_port}" 5 "$(key_name "${prefix}" "map:string")" "map-db1" "mapped db1 key"
  wait_for_absent_db standalone "${dst_port}" 0 "$(key_name "${prefix}" "drop:db")" "db-blacklisted key in db0"
  wait_for_absent_db standalone "${dst_port}" 5 "$(key_name "${prefix}" "drop:db")" "db-blacklisted key in db5"
  assert_no_bisync_metadata_standalone "${dst_port}"

  stop_pid "${SYNCER_PID}"
  SYNCER_PID=""
  shutdown_ports "${src_port}" "${dst_port}"
}

run_slot_filter_matrix() {
  local name=$1
  local mode=$2
  local src_base dst_base http_port
  local src_ports dst_ports src_csv dst_csv prefix conf_file master allow_slot deny_slot filter_block
  local allow_key deny_key

  if [[ "${mode}" == "sync" ]]; then
    src_base=${SYNC_CLUSTER_SRC_BASE}
    dst_base=${SYNC_CLUSTER_DST_BASE}
    http_port=${SYNC_CLUSTER_HTTP_PORT}
  else
    src_base=${PIPE_CLUSTER_SRC_BASE}
    dst_base=${PIPE_CLUSTER_DST_BASE}
    http_port=${PIPE_CLUSTER_HTTP_PORT}
  fi

  src_ports=("${src_base}" "$((src_base + 1))" "$((src_base + 2))")
  dst_ports=("${dst_base}" "$((dst_base + 1))" "$((dst_base + 2))")
  src_csv=$(format_addrs "${src_ports[@]}")
  dst_csv=$(format_addrs "${dst_ports[@]}")
  prefix="${TEST_PREFIX}:${name}:slots"
  echo "[category7] scenario=${name} matrix=slot-filter mode=${mode}"

  start_cluster "${REDIS_SERVER_BIN}" "${TMP_ROOT}" "${name}-slots-src" "${src_ports[@]}"
  start_cluster "${REDIS_SERVER_BIN}" "${TMP_ROOT}" "${name}-slots-dst" "${dst_ports[@]}"
  master=$(find_first_master_port "${src_ports[@]}")
  allow_key="$(key_name "${prefix}" "allow{allow}")"
  deny_key="$(key_name "${prefix}" "deny{deny}")"
  allow_slot=$(cluster_key_slot "${master}" "${allow_key}")
  deny_slot=$(cluster_key_slot "${master}" "${deny_key}")
  if [[ "${allow_slot}" == "${deny_slot}" ]]; then
    deny_key="$(key_name "${prefix}" "deny{deny-alt}")"
    deny_slot=$(cluster_key_slot "${master}" "${deny_key}")
  fi

  filter_block=$(cat <<EOF
  filter:
    slotFilter:
      keySlotWhitelist:
        - [${allow_slot}, ${allow_slot}]
        - [${deny_slot}, ${deny_slot}]
      keySlotBlacklist:
        - [${deny_slot}, ${deny_slot}]
EOF
)
  conf_file="${TMP_ROOT}/${name}-slots.yaml"
  write_matrix_conf "${conf_file}" "${http_port}" "${src_csv}" cluster "${dst_csv}" cluster "${TMP_ROOT}/${name}-slots-store" "${mode}" true replace "" "${filter_block}"
  SYNCER_PID=$(start_syncer_process "${TMP_ROOT}" "${conf_file}" "${TMP_ROOT}/${name}-slots.log")
  wait_for_syncer "${http_port}"
  sleep 2

  redis_cmd cluster "${master}" set "${allow_key}" "allow-value"
  redis_cmd cluster "${master}" set "${deny_key}" "deny-value"

  wait_for_value cluster "$(find_first_master_port "${dst_ports[@]}")" 0 "${allow_key}" "allow-value" "slot-whitelisted key"
  wait_for_absent_db cluster "$(find_first_master_port "${dst_ports[@]}")" 0 "${deny_key}" "slot-blacklisted key"
  assert_no_bisync_metadata_cluster "${dst_ports[@]}"

  stop_pid "${SYNCER_PID}"
  SYNCER_PID=""
  shutdown_ports "${src_ports[@]}" "${dst_ports[@]}"
}

echo "[1/1] building binaries"
build_nonbisync_binaries "${TMP_ROOT}"

case ",${SCENARIOS}," in
  *",sync,"*)
    run_key_exists_matrix sync sync
    run_filter_and_dbmap_matrix sync sync
    run_slot_filter_matrix sync sync
    ;;
esac
case ",${SCENARIOS}," in
  *",pipeline,"*)
    run_key_exists_matrix pipeline pipeline
    run_filter_and_dbmap_matrix pipeline pipeline
    run_slot_filter_matrix pipeline pipeline
    ;;
esac
