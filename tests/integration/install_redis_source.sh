#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/bisync/lib/redis_env.sh"
require_test_commands curl tar make install

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <redis-version> <install-dir>" >&2
  exit 2
fi

VERSION=$1
INSTALL_DIR=$2
WORK_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/redis-source-build.XXXXXX")

cleanup() {
  local code=$?
  if [[ -n "${WORK_ROOT}" && -d "${WORK_ROOT}" ]]; then
    rm -rf -- "${WORK_ROOT}"
  fi
  exit "${code}"
}
trap cleanup EXIT

case "${VERSION}" in
  *[!0-9.]*|'')
    echo "invalid Redis version: ${VERSION}" >&2
    exit 2
    ;;
esac

mkdir -p "${INSTALL_DIR}/bin"
curl --fail --location --retry 3 \
  "https://github.com/redis/redis/archive/refs/tags/${VERSION}.tar.gz" \
  -o "${WORK_ROOT}/redis.tar.gz"
tar -xzf "${WORK_ROOT}/redis.tar.gz" -C "${WORK_ROOT}"
SOURCE_DIR="${WORK_ROOT}/redis-${VERSION}"
if [[ ! -d "${SOURCE_DIR}" ]]; then
  echo "Redis source directory was not created: ${SOURCE_DIR}" >&2
  exit 1
fi

make -C "${SOURCE_DIR}" -j"${REDIS_BUILD_JOBS:-2}" MALLOC=libc BUILD_TLS=yes
for binary in redis-server redis-cli redis-check-aof redis-check-rdb; do
  install -m 0755 "${SOURCE_DIR}/src/${binary}" "${INSTALL_DIR}/bin/${binary}"
done

"${INSTALL_DIR}/bin/redis-server" --version
