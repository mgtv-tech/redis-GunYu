#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/nonbisync/lib/test_env.sh"
require_test_commands go redis-server redis-cli curl git

PREVIOUS_GUNYU_BIN="${PREVIOUS_GUNYU_BIN:-}"
CURRENT_GUNYU_BIN="${CURRENT_GUNYU_BIN:-}"
REDIS_SERVER_BIN="$(resolve_redis_server_bin REDIS_SERVER_BIN REDIS_DEPLOY_ROOT)"
RUN_ID="${TEST_RUN_ID:-$(date -u '+%Y%m%dT%H%M%SZ')-$$}"
ARTIFACT_BASE="${ARTIFACT_ROOT:-${ROOT}/.artifacts/tests/upgrade-rollback}"
ARTIFACT_ROOT="${ARTIFACT_BASE%/}/${RUN_ID}"
REDIS_ROOT="${ARTIFACT_ROOT}/redis"
BIN_ROOT="${ARTIFACT_ROOT}/bin"
STORE_ROOT="${ARTIFACT_ROOT}/store"
UPGRADE_PORT_BASE="${UPGRADE_PORT_BASE:-}"
SYNCER_PID=""
mkdir -p "${REDIS_ROOT}" "${BIN_ROOT}" "${STORE_ROOT}"

if [[ -z "${PREVIOUS_GUNYU_BIN}" || ! -x "${PREVIOUS_GUNYU_BIN}" ]]; then
  echo "PREVIOUS_GUNYU_BIN must point to an executable previous release" >&2
  exit 2
fi
if [[ -z "${CURRENT_GUNYU_BIN}" ]]; then
  CURRENT_GUNYU_BIN="${BIN_ROOT}/redisGunYu-current"
  (cd "${ROOT}" && go build -o "${CURRENT_GUNYU_BIN}" ./main.go)
elif [[ ! -x "${CURRENT_GUNYU_BIN}" ]]; then
  echo "CURRENT_GUNYU_BIN is not executable: ${CURRENT_GUNYU_BIN}" >&2
  exit 2
fi

port_is_open() { (echo >/dev/tcp/127.0.0.1/"$1") >/dev/null 2>&1; }
choose_base() {
  local candidate attempt
  if [[ -n "${UPGRADE_PORT_BASE}" ]]; then
    candidate=${UPGRADE_PORT_BASE}
    if port_is_open "${candidate}" || port_is_open "$((candidate + 1))" || port_is_open "$((candidate + 2))"; then
      return 1
    fi
    printf '%s\n' "${candidate}"
    return 0
  fi
  for attempt in $(seq 1 30); do
    candidate=$((22000 + RANDOM % 25000))
    if ! port_is_open "${candidate}" && ! port_is_open "$((candidate + 1))" && ! port_is_open "$((candidate + 2))"; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done
  return 1
}

stop_syncer() {
  if [[ -n "${SYNCER_PID}" ]]; then
    kill "${SYNCER_PID}" >/dev/null 2>&1 || true
    wait "${SYNCER_PID}" >/dev/null 2>&1 || true
    SYNCER_PID=""
  fi
}

cleanup() {
  local code=$?
  set +e
  stop_syncer
  local pid_file pid
  while IFS= read -r pid_file; do
    pid=$(tr -d '[:space:]' < "${pid_file}")
    if [[ "${pid}" =~ ^[0-9]+$ ]] && kill -0 "${pid}" >/dev/null 2>&1; then
      kill "${pid}" >/dev/null 2>&1 || true
    fi
  done < <(find "${REDIS_ROOT}" -type f -name redis.pid 2>/dev/null)
  exit "${code}"
}
trap cleanup EXIT

BASE=$(choose_base)
SRC_PORT=${BASE}
DST_PORT=$((BASE + 1))
HTTP_PORT=$((BASE + 2))
start_standalone "${REDIS_SERVER_BIN}" "${REDIS_ROOT}" source "${SRC_PORT}"
start_standalone "${REDIS_SERVER_BIN}" "${REDIS_ROOT}" target "${DST_PORT}"

CONF_FILE="${ARTIFACT_ROOT}/syncer.yaml"
write_syncer_conf "${CONF_FILE}" "${HTTP_PORT}" "127.0.0.1:${SRC_PORT}" standalone "127.0.0.1:${DST_PORT}" standalone "${STORE_ROOT}" sync
PREFIX="upgrade-rollback:${RUN_ID}"

start_version() {
  local binary=$1
  local label=$2
  "${binary}" -conf "${CONF_FILE}" -cmd sync >"${ARTIFACT_ROOT}/${label}.log" 2>&1 &
  SYNCER_PID=$!
  wait_for_syncer "${HTTP_PORT}"
  sleep 1
}

wait_for_target_value() {
  local key=$1
  local expected=$2
  for _ in $(seq 1 120); do
    if [[ "$(redis-cli -p "${DST_PORT}" --raw get "${key}" 2>/dev/null || true)" == "${expected}" ]]; then
      return 0
    fi
    sleep 0.25
  done
  echo "target did not converge: key=${key} expected=${expected}" >&2
  return 1
}

start_version "${PREVIOUS_GUNYU_BIN}" previous-initial
redis-cli -p "${SRC_PORT}" set "${PREFIX}:before-upgrade" old >/dev/null
wait_for_target_value "${PREFIX}:before-upgrade" old
stop_syncer

redis-cli -p "${SRC_PORT}" set "${PREFIX}:during-upgrade" current >/dev/null
start_version "${CURRENT_GUNYU_BIN}" current
wait_for_target_value "${PREFIX}:during-upgrade" current
stop_syncer

redis-cli -p "${SRC_PORT}" set "${PREFIX}:during-rollback" previous >/dev/null
start_version "${PREVIOUS_GUNYU_BIN}" previous-rollback
wait_for_target_value "${PREFIX}:during-rollback" previous

for suffix in before-upgrade during-upgrade during-rollback; do
  source_value=$(redis-cli -p "${SRC_PORT}" --raw get "${PREFIX}:${suffix}")
  target_value=$(redis-cli -p "${DST_PORT}" --raw get "${PREFIX}:${suffix}")
  if [[ "${source_value}" != "${target_value}" ]]; then
    echo "final mismatch for ${suffix}: source=${source_value} target=${target_value}" >&2
    exit 1
  fi
done

cat > "${ARTIFACT_ROOT}/summary.md" <<EOF
# Upgrade And Rollback Checkpoint Regression

- Status: PASS
- Previous binary: ${PREVIOUS_GUNYU_BIN}
- Current binary: ${CURRENT_GUNYU_BIN}
- Redis: $("${REDIS_SERVER_BIN}" --version)
- Prefix: ${PREFIX}
- Checks: previous -> current resume, current -> previous rollback resume, final data equality
EOF

echo "artifact_root=${ARTIFACT_ROOT}"
