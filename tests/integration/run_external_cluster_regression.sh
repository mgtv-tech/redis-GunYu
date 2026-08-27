#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/nonbisync/lib/test_env.sh"
require_test_commands go redis-server redis-cli curl git

RUN_ID="${TEST_RUN_ID:-$(date -u '+%Y%m%dT%H%M%SZ')-$$}"
ARTIFACT_BASE="${ARTIFACT_ROOT:-${ROOT}/.artifacts/tests/external-cluster}"
ARTIFACT_ROOT="${ARTIFACT_BASE%/}/${RUN_ID}"
REDIS_ROOT="${ARTIFACT_ROOT}/redis"
REDIS_SERVER_BIN="$(resolve_redis_server_bin REDIS_SERVER_BIN REDIS_DEPLOY_ROOT)"
EXTERNAL_CASES="${EXTERNAL_CASES:-mixed,soak}"
EXTERNAL_SOAK_DURATION="${EXTERNAL_SOAK_DURATION:-2m}"
EXTERNAL_PORT_BASE="${EXTERNAL_PORT_BASE:-}"
mkdir -p "${REDIS_ROOT}"

IFS=',' read -r -a requested_external_cases <<< "${EXTERNAL_CASES}"
if [[ ${#requested_external_cases[@]} -eq 0 ]]; then
  echo "EXTERNAL_CASES must select mixed and/or soak" >&2
  exit 2
fi
for requested_case in "${requested_external_cases[@]}"; do
  case "${requested_case}" in
    mixed|soak) ;;
    *) echo "unknown EXTERNAL_CASES entry: ${requested_case}" >&2; exit 2 ;;
  esac
done

port_is_open() {
  (echo >/dev/tcp/127.0.0.1/"$1") >/dev/null 2>&1
}

choose_base() {
  local candidate attempt offset
  if [[ -n "${EXTERNAL_PORT_BASE}" ]]; then
    candidate=${EXTERNAL_PORT_BASE}
    for offset in 0 1 2 3 4 5; do
      if port_is_open "$((candidate + offset))" || port_is_open "$((candidate + offset + 10000))"; then
        echo "external cluster port block is occupied" >&2
        return 1
      fi
    done
    printf '%s\n' "${candidate}"
    return 0
  fi
  for attempt in $(seq 1 30); do
    candidate=$((21000 + RANDOM % 17000))
    local free=1
    for offset in 0 1 2 3 4 5; do
      if port_is_open "$((candidate + offset))" || port_is_open "$((candidate + offset + 10000))"; then
        free=0
      fi
    done
    if [[ ${free} -eq 1 ]]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done
  return 1
}

cleanup() {
  local code=$?
  set +e
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
LEFT_PORTS=("${BASE}" "$((BASE + 1))" "$((BASE + 2))")
RIGHT_PORTS=("$((BASE + 3))" "$((BASE + 4))" "$((BASE + 5))")
start_cluster "${REDIS_SERVER_BIN}" "${REDIS_ROOT}" left "${LEFT_PORTS[@]}"
start_cluster "${REDIS_SERVER_BIN}" "${REDIS_ROOT}" right "${RIGHT_PORTS[@]}"
LEFT_ADDRS=$(format_addrs "${LEFT_PORTS[@]}")
RIGHT_ADDRS=$(format_addrs "${RIGHT_PORTS[@]}")

COMMON_ENV=(
  REDIS_SERVER_BIN="${REDIS_SERVER_BIN}"
  LEFT_ADDRS="${LEFT_ADDRS}"
  RIGHT_ADDRS="${RIGHT_ADDRS}"
  ALLOW_DESTRUCTIVE_REDIS_TESTS=1
  TEST_ENVIRONMENT_ID="owned-external-regression-${RUN_ID}"
  KEEP_TMP=1
)

FAILURES=0
: > "${ARTIFACT_ROOT}/status.tsv"
run_case() {
  local name=$1
  shift
  local case_root="${ARTIFACT_ROOT}/${name}"
  mkdir -p "${case_root}"
  if TMPDIR="${case_root}" env "${COMMON_ENV[@]}" "$@" >"${ARTIFACT_ROOT}/${name}.log" 2>&1; then
    printf '%s\tPASS\n' "${name}" >> "${ARTIFACT_ROOT}/status.tsv"
  else
    printf '%s\tFAIL\n' "${name}" >> "${ARTIFACT_ROOT}/status.tsv"
    tail -n 100 "${ARTIFACT_ROOT}/${name}.log" >&2 || true
    FAILURES=$((FAILURES + 1))
  fi
}

case ",${EXTERNAL_CASES}," in
  *,mixed,*) run_case mixed-structures env SCENARIOS="${SCENARIOS:-sync,pipeline,parallel}" bash "${ROOT}/tests/bisync/run_category6.sh" ;;
esac
case ",${EXTERNAL_CASES}," in
  *,soak,*) run_case external-soak env SCENARIOS="${SCENARIOS:-sync,pipeline}" SOAK_DURATION="${EXTERNAL_SOAK_DURATION}" bash "${ROOT}/tests/bisync/run_category7.sh" ;;
esac

{
  echo "# External Cluster Regression"
  echo
  echo "- Redis: $("${REDIS_SERVER_BIN}" --version)"
  echo "- Environment: owned-external-regression-${RUN_ID}"
  echo
  echo "| Case | Status |"
  echo "| --- | --- |"
  while IFS=$'\t' read -r name status; do echo "| ${name} | ${status} |"; done < "${ARTIFACT_ROOT}/status.tsv"
} > "${ARTIFACT_ROOT}/summary.md"

echo "artifact_root=${ARTIFACT_ROOT}"
if [[ ${FAILURES} -ne 0 ]]; then
  exit 1
fi
