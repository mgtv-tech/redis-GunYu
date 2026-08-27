#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
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

port_is_open() {
  (echo >/dev/tcp/127.0.0.1/"$1") >/dev/null 2>&1
}

preflight_script_ports() {
  local script=$1
  local base offset port
  while IFS= read -r base; do
    [[ -n "${base}" ]] || continue
    for offset in 0 1 2 3 4 5; do
      port=$((base + offset))
      if port_is_open "${port}"; then
        echo "refusing to run ${script}: declared test port ${port} is already occupied" >&2
        return 1
      fi
      if port_is_open "$((port + 10000))"; then
        echo "refusing to run ${script}: cluster bus candidate $((port + 10000)) is already occupied" >&2
        return 1
      fi
    done
  done < <(
    awk '/(PORT|BASE)/ {
      line=$0
      while (match(line, /:-[0-9]+/)) {
        print substr(line, RSTART + 2, RLENGTH - 2)
        line=substr(line, RSTART + RLENGTH)
      }
    }' "${script}" | sort -nu
  )
}

run_case() {
  local name=$1
  shift
  local case_root="${ARTIFACT_ROOT}/${name}"
  local argument variable value
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
    if [[ "${argument}" == "${ROOT}"/tests/*.sh ]] && ! preflight_script_ports "${argument}" >"${ARTIFACT_ROOT}/${name}.log" 2>&1; then
      printf '%s\tFAIL\n' "${name}" >> "${ARTIFACT_ROOT}/status.tsv"
      cat "${ARTIFACT_ROOT}/${name}.log" >&2
      FAILURES=$((FAILURES + 1))
      return 0
    fi
  done
  if TMPDIR="${case_root}" KEEP_TMP=1 "$@" >"${ARTIFACT_ROOT}/${name}.log" 2>&1; then
    printf '%s\tPASS\n' "${name}" >> "${ARTIFACT_ROOT}/status.tsv"
  else
    printf '%s\tFAIL\n' "${name}" >> "${ARTIFACT_ROOT}/status.tsv"
    tail -n 100 "${ARTIFACT_ROOT}/${name}.log" >&2 || true
    FAILURES=$((FAILURES + 1))
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
