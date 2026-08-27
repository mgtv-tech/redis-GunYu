#!/usr/bin/env bash
set -euo pipefail

VERSION="${1:-3.5.15}"
INSTALL_DIR="${2:-${PWD}/.artifacts/tools/etcd}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/bisync/lib/redis_env.sh"

OS=$(uname -s)
case "${OS}" in
  Linux) OS=linux ;;
  Darwin) OS=darwin ;;
  *) echo "unsupported operating system: ${OS}" >&2; exit 2 ;;
esac
ARCH=$(uname -m)
case "${ARCH}" in
  x86_64) ARCH=amd64 ;;
  aarch64|arm64) ARCH=arm64 ;;
  *) echo "unsupported architecture: ${ARCH}" >&2; exit 2 ;;
esac
case "${VERSION}" in
  *[!0-9.]*|'') echo "invalid etcd version: ${VERSION}" >&2; exit 2 ;;
esac

case "${OS}" in
  linux)
    ARCHIVE="etcd-v${VERSION}-${OS}-${ARCH}.tar.gz"
    require_test_commands curl tar install
    ;;
  darwin)
    ARCHIVE="etcd-v${VERSION}-${OS}-${ARCH}.zip"
    require_test_commands curl unzip install
    ;;
esac

WORK_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/etcd-install.XXXXXX")
cleanup() {
  local code=$?
  rm -rf -- "${WORK_ROOT}"
  exit "${code}"
}
trap cleanup EXIT

curl --fail --location --retry 3 \
  "https://github.com/etcd-io/etcd/releases/download/v${VERSION}/${ARCHIVE}" \
  -o "${WORK_ROOT}/${ARCHIVE}"
case "${OS}" in
  linux) tar -xzf "${WORK_ROOT}/${ARCHIVE}" -C "${WORK_ROOT}" ;;
  darwin) unzip -q "${WORK_ROOT}/${ARCHIVE}" -d "${WORK_ROOT}" ;;
esac
mkdir -p "${INSTALL_DIR}/bin"
EXTRACTED_DIR="${WORK_ROOT}/etcd-v${VERSION}-${OS}-${ARCH}"
install -m 0755 "${EXTRACTED_DIR}/etcd" "${INSTALL_DIR}/bin/etcd"
install -m 0755 "${EXTRACTED_DIR}/etcdctl" "${INSTALL_DIR}/bin/etcdctl"
"${INSTALL_DIR}/bin/etcd" --version
