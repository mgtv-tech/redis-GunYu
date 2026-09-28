#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/bisync/lib/redis_env.sh"
pin_linux_ephemeral_port_range
SUITE="${NIGHTLY_SUITE:-nonbisync-core}"
if [[ "${SUITE}" == "etcd" && "${ENABLE_ETCD_TESTS:-0}" != "1" ]]; then
  echo "NIGHTLY_SUITE=etcd requires ENABLE_ETCD_TESTS=1" >&2
  exit 2
fi
RUN_ID="${TEST_RUN_ID:-$(date -u '+%Y%m%dT%H%M%SZ')-$$}"
ARTIFACT_BASE="${ARTIFACT_ROOT:-${ROOT}/.artifacts/tests/nightly}"
ARTIFACT_ROOT="${ARTIFACT_BASE%/}/${SUITE}/${RUN_ID}"
mkdir -p "${ARTIFACT_ROOT}"

FAILURES=0
: > "${ARTIFACT_ROOT}/status.tsv"
: > "${ARTIFACT_ROOT}/reproduce.sh"
chmod +x "${ARTIFACT_ROOT}/reproduce.sh"

wait_for_ports_released() {
  local script=$1
  local base offset port
  # Runners shut their own servers down, but a daemonized redis-server needs a
  # moment to release its listen socket. Waiting here keeps the next case from
  # tripping over the previous one's teardown instead of blaming the code.
  for _ in $(seq 1 100); do
    local busy=0
    while IFS= read -r base; do
      [[ -n "${base}" ]] || continue
      for offset in 0 1 2 3 4 5; do
        port=$((base + offset))
        if port_is_open "${port}"; then
          busy=1
          break 2
        fi
      done
    done < <(declared_test_ports "${script}")
    [[ ${busy} -eq 0 ]] && return 0
    sleep 0.5
  done
  return 1
}

run_case() {
  local name=$1
  shift
  local case_root="${ARTIFACT_ROOT}/${name}"
  local argument variable value script=""
  mkdir -p "${case_root}"
  {
    printf 'TMPDIR=%q KEEP_TMP=1 ' "${case_root}"
    for variable in REDIS_SERVER_BIN ETCD_BIN ENABLE_ETCD_TESTS ENABLE_TLS SCENARIOS SMOKE_CASES EXTERNAL_CASES EXTERNAL_SOAK_DURATION; do
      value=${!variable:-}
      if [[ -n "${value}" ]]; then
        printf '%s=%q ' "${variable}" "${value}"
      fi
    done
    printf '%q ' "$@"
    printf '\n'
  } >> "${ARTIFACT_ROOT}/reproduce.sh"
  echo "[nightly] suite=${SUITE} case=${name}"
  for argument in "$@"; do
    if is_repo_test_script "${argument}" "${ROOT}"; then
      script="${argument}"
      # The runner derives a run-unique port block, so an occupied declared
      # port means a stale process or a genuine conflict. That is an
      # environment failure, not a test failure: report it as such instead of
      # recording a misleading FAIL for the case.
      if ! wait_for_ports_released "${argument}"; then
        echo "ERROR: declared test ports for ${argument} are still occupied after 50s" >&2
        echo "ERROR: a previous case leaked a redis-server or syncer process" >&2
        printf '%s\tENV_ERROR\n' "${name}" >> "${ARTIFACT_ROOT}/status.tsv"
        return 1
      fi
    fi
  done
  if TMPDIR="${case_root}" KEEP_TMP=1 "$@" >"${ARTIFACT_ROOT}/${name}.log" 2>&1; then
    printf '%s\tPASS\n' "${name}" >> "${ARTIFACT_ROOT}/status.tsv"
  else
    printf '%s\tFAIL\n' "${name}" >> "${ARTIFACT_ROOT}/status.tsv"
    tail -n 100 "${ARTIFACT_ROOT}/${name}.log" >&2 || true
    FAILURES=$((FAILURES + 1))
  fi
  if [[ -n "${script}" ]] && ! wait_for_ports_released "${script}"; then
    echo "WARNING: ${name} left a listener on a declared test port; next case may be affected" >&2
  fi
}

case "${SUITE}" in
  nonbisync-core)
    for category in 1 2 3 4 5 6 7 8; do
      run_case "nonbisync-category${category}" env SCENARIOS="${SCENARIOS:-sync,pipeline}" bash "${ROOT}/tests/nonbisync/run_category${category}.sh"
    done
    ;;
  nonbisync-resilience)
    run_case nonbisync-category9 env SCENARIOS="${SCENARIOS:-sync,pipeline}" bash "${ROOT}/tests/nonbisync/run_category9.sh"
    run_case nonbisync-category10 env SCENARIOS="${SCENARIOS:-sync,pipeline}" bash "${ROOT}/tests/nonbisync/run_category10.sh"
    ;;
  etcd)
    run_case nonbisync-etcd env ENABLE_ETCD_TESTS=1 REQUIRE_ETCD_INTEGRATION=1 bash "${ROOT}/tests/nonbisync/run_controlplane_etcd.sh"
    run_case bisync-etcd env ENABLE_ETCD_TESTS=1 REQUIRE_ETCD_INTEGRATION=1 bash "${ROOT}/tests/bisync/run_controlplane_etcd.sh"
    ;;
  bisync-core)
    for category in 1 2 3 4 5 8; do
      run_case "bisync-category${category}" env SCENARIOS="${SCENARIOS:-sync,pipeline,parallel}" bash "${ROOT}/tests/bisync/run_category${category}.sh"
    done
    ;;
  external-cluster)
    run_case external-cluster env ARTIFACT_ROOT="${ARTIFACT_ROOT}/external" bash "${ROOT}/tests/integration/run_external_cluster_regression.sh"
    ;;
  security)
    run_case security env ENABLE_TLS="${ENABLE_TLS:-1}" bash "${ROOT}/tests/nonbisync/run_security_matrix.sh"
    ;;
  modules)
    run_case bisync-modules bash "${ROOT}/tests/bisync/run_category10.sh"
    run_case nonbisync-modules bash "${ROOT}/tests/nonbisync/run_category11.sh"
    ;;
  compatibility)
    run_case go-integration env ARTIFACT_ROOT="${ARTIFACT_ROOT}/go-integration" bash "${ROOT}/tests/integration/run_go_integration.sh"
    run_case e2e-smoke env ARTIFACT_ROOT="${ARTIFACT_ROOT}/e2e-smoke" bash "${ROOT}/tests/integration/run_e2e_smoke.sh"
    ;;
  *)
    echo "unknown NIGHTLY_SUITE=${SUITE}" >&2
    exit 2
    ;;
esac

{
  echo "# Nightly Regression"
  echo
  echo "- Suite: ${SUITE}"
  echo "- Commit: $(git -C "${ROOT}" rev-parse HEAD)"
  echo "- Reproduction commands: \`${ARTIFACT_ROOT}/reproduce.sh\`"
  echo
  echo "| Case | Status |"
  echo "| --- | --- |"
  while IFS=$'\t' read -r name status; do echo "| ${name} | ${status} |"; done < "${ARTIFACT_ROOT}/status.tsv"
} > "${ARTIFACT_ROOT}/summary.md"

echo "artifact_root=${ARTIFACT_ROOT}"
if [[ ${FAILURES} -ne 0 ]]; then
  exit 1
fi
