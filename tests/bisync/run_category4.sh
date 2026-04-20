#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_ROOT="${TMPDIR:-/tmp}/redisgunyu-bisync-cat4"
source "${ROOT}/tests/bisync/lib/redis_env.sh"
KEYSPEC_VERIFY_ADDRS="${KEYSPEC_VERIFY_ADDRS:-}"
KEYSPEC_VERIFY_TAGS="${KEYSPEC_VERIFY_TAGS:-}"
KEYSPEC_VERIFY_SAMPLES_FILE="${KEYSPEC_VERIFY_SAMPLES_FILE:-}"
KEYSPEC_VERIFY_EXTRA_ARGS="${KEYSPEC_VERIFY_EXTRA_ARGS:-}"
KEYSPEC_REDIS_SERVER_ARGS="${KEYSPEC_REDIS_SERVER_ARGS:-}"
KEYSPEC_FAIL_ON_UNSUPPORTED="${KEYSPEC_FAIL_ON_UNSUPPORTED:-0}"
LOCAL_CLUSTER_PORTS=("${KEYSPEC_PORT_1:-30100}" "${KEYSPEC_PORT_2:-30101}" "${KEYSPEC_PORT_3:-30102}")
STARTED_LOCAL_CLUSTER=0
REDIS_SERVER_BIN="$(resolve_redis_server_bin KEYSPEC_REDIS_SERVER KEYSPEC_REDIS_DEPLOY_ROOT)"
if [[ -z "${KEYSPEC_VERIFY_ADDRS}" && -n "${KEYSPEC_VERIFY_DEPLOY_ROOT:-}" ]]; then
  KEYSPEC_VERIFY_ADDRS="$(resolve_deploy_addrs KEYSPEC_VERIFY_DEPLOY_ROOT KEYSPEC_VERIFY_HOST)"
fi

cleanup() {
  local code=$?
  set +e
  if [[ "${STARTED_LOCAL_CLUSTER}" == "1" ]]; then
    shutdown_ports "${LOCAL_CLUSTER_PORTS[@]}"
  fi
  if [[ "${KEEP_TMP:-0}" != "1" ]]; then
    rm -rf "${TMP_ROOT}"
  fi
  exit "${code}"
}
trap cleanup EXIT

shutdown_ports() {
  local port
  for port in "$@"; do
    redis-cli -p "${port}" shutdown nosave >/dev/null 2>&1 || true
  done
}

rm -rf "${TMP_ROOT}"
mkdir -p "${TMP_ROOT}"

build_binaries() {
  echo "[1/4] building verifier"
  mkdir -p "${TMP_ROOT}/gocache"
  (cd "${ROOT}" && GOCACHE="${TMP_ROOT}/gocache" go build -o "${TMP_ROOT}/keyspec_verify" ./tests/bisync/cmd/keyspec_verify)
}

run_unit_tests() {
  echo "[2/4] running filter and routing unit tests"
  (cd "${ROOT}" && GOCACHE="${TMP_ROOT}/gocache" go test ./pkg/filter -run 'TestCommandKeys|TestFilterCmdKeyRejectsUnsafeProjection|TestFilterCmdKeyAppliesSlotOnlyFilters' -count=1)
  (cd "${ROOT}" && GOCACHE="${TMP_ROOT}/gocache" go test ./pkg/redis/client/cluster -run 'TestResolveCommandKeysPrefersStaticTable|TestChooseNodeWithCmdStrictUsesCommandGetKeys|TestChooseNodeWithCmdStrictRejectsCrossSlotFallbackKeys|TestChooseNodeWithCmdFallsBackToFirstArgForCompatibility|TestTxnBatcherRejectsDifferentSlotsOnSameNode|TestTxnBatcherAcceptsSameSlotCommands|TestClusterHandleMoveRefreshesUnknownTargetNode|TestTxnBatcherRetriesOnAsk' -count=1)
  (cd "${ROOT}" && GOCACHE="${TMP_ROOT}/gocache" go test ./syncer -run 'TestResolveBisyncCommandKeysFallsBackToRedisSpec|TestResolveBisyncCommandKeysPrefersStaticTable|TestBuildBisyncReplayUnitWithResolverUsesFallbackKeys|TestBuildBisyncReplayUnitWithResolverCrossSlotFailsViaFallback|TestParseAofReplayUnitsProjectsFilteredTxn|TestParseAofReplayUnitsStandaloneAllowsCrossSlotTxn' -count=1)
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

redis_server_extra_args=()
if [[ -n "${KEYSPEC_REDIS_SERVER_ARGS}" ]]; then
  read -r -a redis_server_extra_args <<< "${KEYSPEC_REDIS_SERVER_ARGS}"
fi

keyspec_verify_extra_args=()
if [[ -n "${KEYSPEC_VERIFY_EXTRA_ARGS}" ]]; then
  read -r -a keyspec_verify_extra_args <<< "${KEYSPEC_VERIFY_EXTRA_ARGS}"
fi

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
    if redis-cli -p "${port}" cluster info 2>/dev/null | rg -q '^cluster_state:ok'; then
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
  echo "[3/4] starting keyspec verification cluster on ports ${ports[*]} via ${REDIS_SERVER_BIN}"
  local port
  for port in "${ports[@]}"; do
    write_redis_conf "${TMP_ROOT}/${prefix}-${port}" "${port}"
    if [[ ${#redis_server_extra_args[@]} -gt 0 ]]; then
      "${REDIS_SERVER_BIN}" "${TMP_ROOT}/${prefix}-${port}/redis.conf" "${redis_server_extra_args[@]}"
    else
      "${REDIS_SERVER_BIN}" "${TMP_ROOT}/${prefix}-${port}/redis.conf"
    fi
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
  STARTED_LOCAL_CLUSTER=1
}

run_keyspec_verify() {
  local addrs=$1
  local log_name=$2
  local cmd=("${TMP_ROOT}/keyspec_verify" "--addrs" "${addrs}")
  if [[ -n "${KEYSPEC_VERIFY_TAGS}" ]]; then
    cmd+=("--tags" "${KEYSPEC_VERIFY_TAGS}")
  fi
  if [[ -n "${KEYSPEC_VERIFY_SAMPLES_FILE}" ]]; then
    cmd+=("--samples-file" "${KEYSPEC_VERIFY_SAMPLES_FILE}")
  fi
  if [[ "${KEYSPEC_FAIL_ON_UNSUPPORTED}" == "1" ]]; then
    cmd+=("--fail-on-unsupported")
  fi
  if [[ ${#keyspec_verify_extra_args[@]} -gt 0 ]]; then
    cmd+=("${keyspec_verify_extra_args[@]}")
  fi

  echo "[4/4] running real Redis keyspec verification against ${addrs}"
  "${cmd[@]}" | tee "${TMP_ROOT}/${log_name}"
}

build_binaries
run_unit_tests
if [[ -n "${KEYSPEC_VERIFY_ADDRS}" ]]; then
  run_keyspec_verify "${KEYSPEC_VERIFY_ADDRS}" "keyspec_verify.log"
else
  start_cluster keyspec "${LOCAL_CLUSTER_PORTS[@]}"
  run_keyspec_verify "127.0.0.1:${LOCAL_CLUSTER_PORTS[0]}" "keyspec_verify.log"
fi

echo "keyspec_log=${TMP_ROOT}/keyspec_verify.log"
