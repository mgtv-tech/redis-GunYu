#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ARTIFACT_ROOT="${ARTIFACT_ROOT:-${ROOT}/.artifacts/tests/sentinel-security-matrix}"
PORT_BASE="${SENTINEL_SECURITY_PORT_BASE:-42000}"
mkdir -p "${ARTIFACT_ROOT}"

run_case() {
  local name=$1 data_tls=$2 sentinel_tls=$3 base=$4
  local case_root="${ARTIFACT_ROOT}/${name}"
  mkdir -p "${case_root}"
  if env \
    SENTINEL_SECURITY_CASE="${name}" \
    SENTINEL_DATA_TLS="${data_tls}" \
    SENTINEL_TLS="${sentinel_tls}" \
    SENTINEL_SECURITY_PORT_BASE="${base}" \
    TMPDIR="${case_root}" \
    bash "${ROOT}/tests/integration/run_sentinel_security_case.sh" \
    >"${case_root}.log" 2>&1; then
    printf '%s\tPASS\n' "${name}" >>"${ARTIFACT_ROOT}/status.tsv"
  else
    printf '%s\tFAIL\n' "${name}" >>"${ARTIFACT_ROOT}/status.tsv"
    tail -n 100 "${case_root}.log" >&2 || true
    return 1
  fi
}

: >"${ARTIFACT_ROOT}/status.tsv"
failures=0
run_case acl 0 0 "${PORT_BASE}" || failures=$((failures + 1))
run_case sentinel-tls 0 1 "$((PORT_BASE + 20))" || failures=$((failures + 1))
run_case data-tls 1 0 "$((PORT_BASE + 40))" || failures=$((failures + 1))
run_case both-tls 1 1 "$((PORT_BASE + 60))" || failures=$((failures + 1))

{
  echo "# Sentinel Security Matrix"
  echo
  echo "- Redis: $("${REDIS_SERVER_BIN:-redis-server}" --version)"
  echo
  echo "| Case | Status |"
  echo "| --- | --- |"
  while IFS=$'\t' read -r name status; do echo "| ${name} | ${status} |"; done <"${ARTIFACT_ROOT}/status.tsv"
} >"${ARTIFACT_ROOT}/summary.md"

if ((failures > 0)); then
  echo "sentinel security matrix failures=${failures}" >&2
  exit 1
fi
echo "sentinel_security_matrix=${ARTIFACT_ROOT}/summary.md"
