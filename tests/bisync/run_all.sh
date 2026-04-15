#!/usr/bin/env bash
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_ROOT="${TMPDIR:-/tmp}/redisgunyu-bisync-all"
REPORT_FILE="${TMP_ROOT}/report.md"
mkdir -p "${TMP_ROOT}"

CASE_NAMES=()
CASE_STATUSES=()
CASE_DURATIONS=()
CASE_LOGS=()
HAS_FAILURE=0

run_case() {
  local name=$1
  shift
  local log_file="${TMP_ROOT}/${name}.log"
  local start_ts end_ts duration status

  start_ts=$(date +%s)
  if (cd "${ROOT}" && "$@") >"${log_file}" 2>&1; then
    status="PASS"
  else
    status="FAIL"
  fi
  end_ts=$(date +%s)
  duration=$((end_ts - start_ts))

  CASE_NAMES+=("${name}")
  CASE_STATUSES+=("${status}")
  CASE_DURATIONS+=("${duration}")
  CASE_LOGS+=("${log_file}")
  if [[ "${status}" == "FAIL" ]]; then
    HAS_FAILURE=1
  fi
}

write_report() {
  {
    echo "# Bisync All Tests Report"
    echo
    echo "- GeneratedAt: $(date '+%Y-%m-%d %H:%M:%S %z')"
    echo "- Workspace: ${ROOT}"
    echo "- Overall: $([[ "${HAS_FAILURE}" == "0" ]] && echo PASS || echo FAIL)"
    echo
    echo "## Summary"
    echo
    echo "| Case | Status | DurationSeconds | Log |"
    echo "| --- | --- | ---: | --- |"
    local i
    for i in "${!CASE_NAMES[@]}"; do
      echo "| ${CASE_NAMES[$i]} | ${CASE_STATUSES[$i]} | ${CASE_DURATIONS[$i]} | ${CASE_LOGS[$i]} |"
    done
    echo
    echo "## Tail Logs"
    echo
    for i in "${!CASE_NAMES[@]}"; do
      echo "### ${CASE_NAMES[$i]}"
      echo
      echo "\`\`\`text"
      tail -n 40 "${CASE_LOGS[$i]}" || true
      echo "\`\`\`"
      echo
    done
  } > "${REPORT_FILE}"
}

run_case category1 bash ./tests/bisync/run_category1.sh
run_case category2 bash ./tests/bisync/run_category2.sh
run_case category3 bash ./tests/bisync/run_category3.sh
run_case category4 bash ./tests/bisync/run_category4.sh
run_case category5 bash ./tests/bisync/run_category5.sh
run_case category6 bash ./tests/bisync/run_category6.sh
run_case category7 bash ./tests/bisync/run_category7.sh
run_case category8 bash ./tests/bisync/run_category8.sh

write_report
echo "report=${REPORT_FILE}"

if [[ "${HAS_FAILURE}" == "1" ]]; then
  exit 1
fi
