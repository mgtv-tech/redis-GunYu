#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/bisync/lib/redis_env.sh"

SYSTEM_GREP="$(command -v grep)"
SYSTEM_RM="$(command -v rm)"
TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/redisgunyu-portability.XXXXXX")"
cleanup() {
  "${SYSTEM_RM}" -rf "${TMP_ROOT}"
}
trap cleanup EXIT

mkdir -p "${TMP_ROOT}/bin"
ln -s "${SYSTEM_GREP}" "${TMP_ROOT}/bin/grep"
printf '%s\n' alpha beta beta gamma > "${TMP_ROOT}/fixture.txt"

# Force the grep fallback even on development machines that have ripgrep.
ORIGINAL_PATH="${PATH}"
PATH="${TMP_ROOT}/bin"
export PATH

match_regex_quiet '^beta$' "${TMP_ROOT}/fixture.txt"
[[ "$(count_regex_matches '^beta$' "${TMP_ROOT}/fixture.txt")" == "2" ]]
[[ "$(print_regex_matches '^beta$' "${TMP_ROOT}/fixture.txt")" == $'2:beta\n3:beta' ]]
[[ "$(printf '%s\n' alpha beta | exclude_regex_matches '^beta$')" == "alpha" ]]

REDIS_SERVER_BIN="/bin/sh"
require_test_commands redis-server

if missing_output="$(require_test_commands redisgunyu-command-that-does-not-exist 2>&1)"; then
  echo "missing dependency check unexpectedly succeeded" >&2
  exit 1
fi
[[ "${missing_output}" == *"missing required tool: redisgunyu-command-that-does-not-exist"* ]]
[[ "${missing_output}" != *"brew install"* ]]

PATH="${ORIGINAL_PATH}"
export PATH

ETCD_DEFAULT_TMP="${TMP_ROOT}/etcd-default"
mkdir -p "${ETCD_DEFAULT_TMP}"
nonbisync_etcd_output="$(TMPDIR="${ETCD_DEFAULT_TMP}" bash "${ROOT}/tests/nonbisync/run_controlplane_etcd.sh")"
bisync_etcd_output="$(TMPDIR="${ETCD_DEFAULT_TMP}" bash "${ROOT}/tests/bisync/run_controlplane_etcd.sh")"
[[ "${nonbisync_etcd_output}" == *"etcd control-plane tests are disabled"* ]]
[[ "${bisync_etcd_output}" == *"etcd control-plane tests are disabled"* ]]
[[ ! -e "${ETCD_DEFAULT_TMP}/redisgunyu-nonbisync-etcd" ]]
[[ ! -e "${ETCD_DEFAULT_TMP}/redisgunyu-bisync-etcd" ]]

if nightly_etcd_output="$(NIGHTLY_SUITE=etcd ARTIFACT_ROOT="${TMP_ROOT}/nightly-default" bash "${ROOT}/tests/integration/run_nightly.sh" 2>&1)"; then
  echo "disabled nightly etcd suite unexpectedly succeeded" >&2
  exit 1
fi
[[ "${nightly_etcd_output}" == *"requires ENABLE_ETCD_TESTS=1"* ]]
[[ ! -e "${TMP_ROOT}/nightly-default" ]]

echo "platform compatibility helpers passed"
