#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/bisync/lib/redis_env.sh"
require_test_commands

RUN_ID="${TEST_RUN_ID:-$(date -u '+%Y%m%dT%H%M%SZ')-$$}"
ARTIFACT_BASE="${ARTIFACT_ROOT:-${ROOT}/.artifacts/tests/e2e-smoke}"
ARTIFACT_ROOT="${ARTIFACT_BASE%/}/${RUN_ID}"
SMOKE_CASES="${SMOKE_CASES:-nonbisync,bisync}"
SMOKE_PORT_BASE="${SMOKE_PORT_BASE:-}"
REDIS_SERVER_BIN="$(resolve_redis_server_bin REDIS_SERVER_BIN REDIS_DEPLOY_ROOT)"
mkdir -p "${ARTIFACT_ROOT}"

IFS=',' read -r -a requested_smoke_cases <<< "${SMOKE_CASES}"
if [[ ${#requested_smoke_cases[@]} -eq 0 ]]; then
  echo "SMOKE_CASES must select nonbisync and/or bisync" >&2
  exit 2
fi
for requested_case in "${requested_smoke_cases[@]}"; do
  case "${requested_case}" in
    nonbisync|bisync) ;;
    *) echo "unknown SMOKE_CASES entry: ${requested_case}" >&2; exit 2 ;;
  esac
done

port_is_open() {
  local port=$1
  (echo >/dev/tcp/127.0.0.1/"${port}") >/dev/null 2>&1
}

candidate_is_free() {
  local base=$1
  local offset port
  for offset in $(seq 0 60); do
    port=$((base + offset))
    if port_is_open "${port}" || port_is_open "$((port + 10000))"; then
      return 1
    fi
  done
}

choose_port_base() {
  local candidate attempt
  if [[ -n "${SMOKE_PORT_BASE}" ]]; then
    if ! candidate_is_free "${SMOKE_PORT_BASE}"; then
      echo "smoke port block is occupied: ${SMOKE_PORT_BASE}-$((SMOKE_PORT_BASE + 60))" >&2
      return 1
    fi
    printf '%s\n' "${SMOKE_PORT_BASE}"
    return 0
  fi
  for attempt in $(seq 1 30); do
    candidate=$((20000 + RANDOM % 16000))
    if candidate_is_free "${candidate}"; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done
  echo "could not find a free E2E port block" >&2
  return 1
}

run_case() {
  local name=$1
  shift
  local case_root="${ARTIFACT_ROOT}/${name}"
  local log_file="${ARTIFACT_ROOT}/${name}.log"
  mkdir -p "${case_root}"
  echo "[e2e-smoke] case=${name}"
  if TMPDIR="${case_root}" KEEP_TMP=1 REDIS_SERVER_BIN="${REDIS_SERVER_BIN}" "$@" >"${log_file}" 2>&1; then
    printf '%s\tPASS\n' "${name}" >> "${ARTIFACT_ROOT}/status.tsv"
  else
    printf '%s\tFAIL\n' "${name}" >> "${ARTIFACT_ROOT}/status.tsv"
    tail -n 100 "${log_file}" >&2 || true
    return 1
  fi
}

BASE="$(choose_port_base)"
FAILURES=0
: > "${ARTIFACT_ROOT}/status.tsv"

{
  echo "run_id=${RUN_ID}"
  echo "git_commit=$(git -C "${ROOT}" rev-parse HEAD)"
  echo "redis_server=${REDIS_SERVER_BIN}"
  echo "redis_version=$("${REDIS_SERVER_BIN}" --version)"
  echo "smoke_cases=${SMOKE_CASES}"
  echo "port_base=${BASE}"
} > "${ARTIFACT_ROOT}/manifest.txt"

case ",${SMOKE_CASES}," in
  *,nonbisync,*)
    run_case nonbisync-standalone-resume env \
      SCENARIOS=sync \
      SYNC_SRC_PORT=$((BASE + 0)) SYNC_DST_PORT=$((BASE + 1)) SYNC_HTTP_PORT=$((BASE + 2)) \
      PIPE_SRC_PORT=$((BASE + 3)) PIPE_DST_PORT=$((BASE + 4)) PIPE_HTTP_PORT=$((BASE + 5)) \
      bash "${ROOT}/tests/nonbisync/run_category3.sh" || FAILURES=$((FAILURES + 1))

    run_case nonbisync-cluster-pipeline env \
      SCENARIOS=pipeline \
      SYNC_SRC_BASE=$((BASE + 6)) SYNC_DST_BASE=$((BASE + 9)) SYNC_HTTP_PORT=$((BASE + 12)) \
      PIPE_SRC_BASE=$((BASE + 13)) PIPE_DST_BASE=$((BASE + 16)) PIPE_HTTP_PORT=$((BASE + 19)) \
      bash "${ROOT}/tests/nonbisync/run_category1.sh" || FAILURES=$((FAILURES + 1))
    ;;
esac

case ",${SMOKE_CASES}," in
  *,bisync,*)
    run_case bisync-cluster-sync env \
      SRC_PORT_1=$((BASE + 20)) SRC_PORT_2=$((BASE + 21)) SRC_PORT_3=$((BASE + 22)) \
      DST_PORT_1=$((BASE + 23)) DST_PORT_2=$((BASE + 24)) DST_PORT_3=$((BASE + 25)) \
      FWD_HTTP_PORT=$((BASE + 26)) REV_HTTP_PORT=$((BASE + 27)) \
      bash "${ROOT}/tests/bisync/run_category1.sh" || FAILURES=$((FAILURES + 1))

    run_case bisync-pipeline-resume env \
      SCENARIOS=pipeline \
      SERIAL_SRC_BASE=$((BASE + 28)) SERIAL_DST_BASE=$((BASE + 31)) \
      SERIAL_FWD_HTTP_PORT=$((BASE + 34)) SERIAL_REV_HTTP_PORT=$((BASE + 35)) \
      ORDERED_SRC_BASE=$((BASE + 36)) ORDERED_DST_BASE=$((BASE + 39)) \
      ORDERED_FWD_HTTP_PORT=$((BASE + 42)) ORDERED_REV_HTTP_PORT=$((BASE + 43)) \
      PIPELINE_SRC_BASE=$((BASE + 44)) PIPELINE_DST_BASE=$((BASE + 47)) \
      PIPELINE_FWD_HTTP_PORT=$((BASE + 50)) PIPELINE_REV_HTTP_PORT=$((BASE + 51)) \
      bash "${ROOT}/tests/bisync/run_category2.sh" || FAILURES=$((FAILURES + 1))
    ;;
esac

{
  echo "# E2E Smoke Report"
  echo
  echo "- Run ID: ${RUN_ID}"
  echo "- Redis: $("${REDIS_SERVER_BIN}" --version)"
  echo
  echo "| Case | Status |"
  echo "| --- | --- |"
  while IFS=$'\t' read -r name status; do
    echo "| ${name} | ${status} |"
  done < "${ARTIFACT_ROOT}/status.tsv"
} > "${ARTIFACT_ROOT}/summary.md"

echo "artifact_root=${ARTIFACT_ROOT}"
if [[ ${FAILURES} -ne 0 ]]; then
  exit 1
fi
