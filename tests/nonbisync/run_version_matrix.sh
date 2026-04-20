#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_ROOT="${TMPDIR:-/tmp}/redisgunyu-nonbisync-version-matrix"
REDIS_SERVER_BINS="${REDIS_SERVER_BINS:-}"
RUNNERS="${RUNNERS:-./tests/nonbisync/run_all.sh}"
KEEP_TMP="${KEEP_TMP:-0}"

if [[ -z "${REDIS_SERVER_BINS}" ]]; then
  echo "REDIS_SERVER_BINS is required. Example: REDIS_SERVER_BINS=/path/redis-7.0,/path/redis-7.2" >&2
  exit 2
fi

rm -rf "${TMP_ROOT}"
mkdir -p "${TMP_ROOT}"

IFS=',' read -r -a bins <<< "${REDIS_SERVER_BINS}"
IFS=',' read -r -a runners <<< "${RUNNERS}"

for bin in "${bins[@]}"; do
  [[ -n "${bin}" ]] || continue
  if [[ ! -x "${bin}" ]]; then
    echo "redis-server binary is not executable: ${bin}" >&2
    exit 2
  fi

  version_tag=$("${bin}" --version 2>/dev/null | sed -n 's/.* v=\([^ ]*\).*/\1/p' | tr '/ ' '__')
  version_tag=${version_tag:-unknown}
  version_dir="${TMP_ROOT}/${version_tag}"
  mkdir -p "${version_dir}"
  echo "[matrix] redis-server=${bin} version=${version_tag}"

  for runner in "${runners[@]}"; do
    [[ -n "${runner}" ]] || continue
    runner_tag=$(basename "${runner}" .sh)
    log_file="${version_dir}/${runner_tag}.log"
    echo "[matrix] runner=${runner}"
    (
      cd "${ROOT}" && \
      REDIS_SERVER_BIN="${bin}" KEEP_TMP="${KEEP_TMP}" bash "${runner}"
    ) >"${log_file}" 2>&1
  done
done

echo "matrix_root=${TMP_ROOT}"
