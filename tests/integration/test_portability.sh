#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/bisync/lib/redis_env.sh"
source "${ROOT}/tests/lib/test_ports.sh"

SYSTEM_GREP="$(command -v grep)"
SYSTEM_RM="$(command -v rm)"
TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/redisgunyu-portability.XXXXXX")"
cleanup() {
  "${SYSTEM_RM}" -rf "${TMP_ROOT}"
}
trap cleanup EXIT
export TEST_PORT_REGISTRY="${TMP_ROOT}/test-ports.tsv"
unset TEST_PORT_OFFSET TEST_PORT_BLOCK_END

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

[[ "$(cluster_bus_port 19100)" == "29100" ]]
[[ "$(cluster_bus_port 22767)" == "32767" ]]
[[ "$(cluster_bus_port 22768)" == "12768" ]]
[[ "$(cluster_bus_port 31100)" == "21100" ]]
[[ "$(cluster_bus_port 36100)" == "26100" ]]
pin_linux_ephemeral_port_range

# Port derivation: the same category must land on the same block for one run
# and job, different categories must not share a block, and every declared base
# must fall inside the block it was derived from.
test_ports_derive probe-category-a
block_a_start="${TEST_PORT_OFFSET}"
block_a_end="${TEST_PORT_BLOCK_END}"
[[ "$(test_port_at 0)" == "${block_a_start}" ]]
if (( block_a_start < TEST_PORT_RANGE_START )); then
  echo "derived block start ${block_a_start} is below TEST_PORT_RANGE_START=${TEST_PORT_RANGE_START}" >&2
  exit 1
fi
if (( block_a_end > TEST_PORT_BUS_SAFE_END )); then
  echo "derived block end ${block_a_end} exceeds TEST_PORT_BUS_SAFE_END=${TEST_PORT_BUS_SAFE_END}" >&2
  exit 1
fi
if (( block_a_end + 10000 >= 32768 )); then
  echo "cluster bus for data port ${block_a_end} would enter the Linux ephemeral range" >&2
  exit 1
fi
if (( block_a_end - block_a_start != TEST_PORT_SPAN - 1 )); then
  echo "derived block ${block_a_start}-${block_a_end} is not TEST_PORT_SPAN=${TEST_PORT_SPAN} wide" >&2
  exit 1
fi
unset TEST_PORT_OFFSET TEST_PORT_BLOCK_END
test_ports_derive probe-category-a
[[ "${TEST_PORT_OFFSET}" == "${block_a_start}" ]]
unset TEST_PORT_OFFSET TEST_PORT_BLOCK_END
test_ports_derive probe-category-b
[[ "${TEST_PORT_OFFSET}" != "${block_a_start}" ]]
if ! (( TEST_PORT_OFFSET > block_a_end || TEST_PORT_OFFSET + TEST_PORT_SPAN - 1 < block_a_start )); then
  echo "category-b block ${TEST_PORT_OFFSET} overlaps category-a ${block_a_start}-${block_a_end}" >&2
  exit 1
fi

TEST_PORT_OFFSET=50000
TEST_PORT_BLOCK_END=50007
export TEST_PORT_OFFSET TEST_PORT_BLOCK_END
test_ports_assert_declared probe 50000 50007
if test_ports_assert_declared probe 49999 2>/dev/null; then
  echo "off-block declared port unexpectedly passed validation" >&2
  exit 1
fi
unset TEST_PORT_OFFSET TEST_PORT_BLOCK_END

side_dir="${TMP_ROOT}/redis-env-source"
mkdir -p "${side_dir}"
source_output="$(cd "${side_dir}" && bash -c "source '${ROOT}/tests/bisync/lib/redis_env.sh'; find . -mindepth 1 -print")"
[[ -z "${source_output}" ]]

is_repo_test_script "${ROOT}/tests/nonbisync/run_category1.sh" "${ROOT}"
is_repo_test_script "${ROOT}/tests/integration/run_go_integration.sh" "${ROOT}"
if is_repo_test_script "${ROOT}/main.go" "${ROOT}"; then
  echo "main.go should not match the nightly test-script glob" >&2
  exit 1
fi

# declared_test_ports must report the ports a runner will really listen on, not
# the literal defaults it used to hardcode. category4 derives its ports, so the
# resolver has to reproduce that derivation.
declared_cat4_ports="$(declared_test_ports "${ROOT}/tests/bisync/run_category4.sh")"
if [[ -z "${declared_cat4_ports}" ]]; then
  echo "bisync category4 should declare derived ports, got none" >&2
  exit 1
fi
cat4_name="$(awk '
  match($0, /test_ports_derive[ \t]+[^ \t]+/) {
    name = substr($0, RSTART + 17, RLENGTH - 17)
    gsub(/^[ \t]+|[ \t]+$/, "", name)
    print name
    exit
  }
' "${ROOT}/tests/bisync/run_category4.sh")"
TEST_PORT_OFFSET="" TEST_PORT_BLOCK_END="" test_ports_derive "${cat4_name}" >/dev/null
cat4_offset="${TEST_PORT_OFFSET}"
unset TEST_PORT_OFFSET TEST_PORT_BLOCK_END
while IFS= read -r declared; do
  [[ -n "${declared}" ]] || continue
  if (( declared < cat4_offset || declared > cat4_offset + TEST_PORT_SPAN - 1 )); then
    echo "bisync category4 declared port ${declared} is outside its block ${cat4_offset}-$((cat4_offset + TEST_PORT_SPAN - 1))" >&2
    exit 1
  fi
done <<< "${declared_cat4_ports}"

# A runner that does not listen on any port must still resolve to nothing.
declared_go_integration_ports="$(declared_test_ports "${ROOT}/tests/integration/run_go_integration.sh")"
[[ -z "${declared_go_integration_ports}" ]]

# Every category runner must rebase its ports through the shared derivation and
# must not keep a literal 1024-65535 default that the derivation cannot move.
while IFS= read -r file; do
  if ! grep -qE '_PORT|_BASE|PORTS=' "${file}"; then
    continue
  fi
  if ! grep -q 'tests/lib/test_ports.sh' "${file}"; then
    echo "${file} declares ports but does not source tests/lib/test_ports.sh" >&2
    exit 1
  fi
  if ! grep -q '^test_ports_derive ' "${file}"; then
    echo "${file} declares ports but does not call test_ports_derive" >&2
    exit 1
  fi
  if hardcoded="$(grep -nE '_PORT|_BASE|PORTS=' "${file}" | grep -E ':-[0-9]{4,5}' | grep -v 'test_port_at')"; then
    echo "${file} still hardcodes a port default:" >&2
    echo "${hardcoded}" >&2
    exit 1
  fi
  # A runner that declares more bases than the block holds would spill into the
  # neighbouring category's block, so the widest runner bounds TEST_PORT_SPAN.
  widest="$(grep -oE 'test_port_at[ \t]+[0-9]+' "${file}" | awk '{print $2}' | sort -n | tail -1)"
  if [[ -n "${widest}" ]] && (( widest >= TEST_PORT_SPAN )); then
    echo "${file} declares slot index ${widest} but TEST_PORT_SPAN=${TEST_PORT_SPAN}" >&2
    exit 1
  fi
done < <(find "${ROOT}/tests" -name 'run_category*.sh' -type f | sort)

python3 - "${TEST_PORT_SPAN}" \
  "${ROOT}/tests/nonbisync/run_security_matrix.sh" \
  "${ROOT}/tests/nonbisync/run_controlplane_etcd.sh" \
  "${ROOT}/tests/bisync/run_controlplane_etcd.sh" \
  $(find "${ROOT}/tests" -name 'run_category*.sh' -type f | sort) <<'PY'
import re
import sys
from collections import defaultdict
from pathlib import Path

span = int(sys.argv[1])
errors = []
for path in map(Path, sys.argv[2:]):
    text = path.read_text()
    assigns = {}
    for match in re.finditer(r"\$\{([A-Z][A-Z0-9_]*):-\$\(test_port_at (\d+)\)\}", text):
        assigns[match.group(1)] = int(match.group(2))
    if not assigns:
        continue
    plus = {name: 0 for name in assigns}
    for name in assigns:
        for match in re.finditer(r"\$\(\(%s \+ (\d+)\)\)" % re.escape(name), text):
            plus[name] = max(plus[name], int(match.group(1)))
        if name.endswith("_BASE"):
            plus[name] = max(plus[name], 2)
    aliases = defaultdict(set)
    for match in re.finditer(r"\b(src_base|dst_base)\s*=\s*\$\{?([A-Z][A-Z0-9_]*)\}?", text):
        aliases[match.group(1)].add(match.group(2))
    for local, names in aliases.items():
        max_off = 0
        for match in re.finditer(r"\$\(\(%s \+ (\d+)\)\)" % re.escape(local), text):
            max_off = max(max_off, int(match.group(1)))
        for name in names:
            if name in plus:
                plus[name] = max(plus[name], max_off)
    occupied = {}
    for name, slot in assigns.items():
        for offset in range(plus[name] + 1):
            occupied.setdefault(slot + offset, []).append(
                f"{name}+{offset}" if offset else name
            )
    collisions = {slot: names for slot, names in occupied.items() if len(names) > 1}
    if collisions:
        details = ", ".join(
            f"{slot}:{('/'.join(names))}" for slot, names in sorted(collisions.items())[:8]
        )
        errors.append(f"{path}: overlapping listen slots {details}")
    if occupied and max(occupied) >= span:
        errors.append(f"{path}: slot {max(occupied)} is outside TEST_PORT_SPAN={span}")
if errors:
    raise SystemExit("\n".join(errors))
PY

while IFS= read -r file; do
  python3 - "${file}" <<'PY'
import sys
from pathlib import Path
path = Path(sys.argv[1])
lines = path.read_text().splitlines()
for index, line in enumerate(lines):
    if "cluster-enabled yes" not in line:
        continue
    window = "\n".join(lines[index:index + 8])
    if "cluster-port $(cluster_bus_port" not in window:
        raise SystemExit(f"{path}: cluster-enabled without cluster_bus_port at line {index + 1}")
PY
done < <(grep -rl --include='*.sh' --exclude='test_portability.sh' 'cluster-enabled yes' "${ROOT}/tests")

# Category runners must derive their ports from tests/lib/test_ports.sh rather
# than hardcoding a block. Fixed blocks collided across categories (bisync
# category2 with category3, nonbisync category4 with category11) and landed
# inside the Linux ephemeral range, which is what made nightly results alternate
# between pass and fail on an unchanged commit.
while IFS= read -r file; do
  python3 - "${file}" <<'PY'
import re
import sys
from pathlib import Path
path = Path(sys.argv[1])
if "test_ports.sh" not in path.read_text():
    raise SystemExit(f"{path}: category runner does not source tests/lib/test_ports.sh")
for index, line in enumerate(path.read_text().splitlines()):
    if not re.search(r'_PORT|_BASE|PORTS=', line):
        continue
    for match in re.finditer(r':-(\d+)', line):
        port = int(match.group(1))
        if 1024 <= port <= 65535:
            raise SystemExit(
                f"{path}:{index + 1}: hardcoded test port {port}; "
                "use \"${VAR:-$(test_port_at n)}\" instead"
            )
PY
done < <(grep -rl --include='run_category*.sh' '_PORT\|_BASE' "${ROOT}/tests")

cli_dir="${TMP_ROOT}/redis-install/bin"
mkdir -p "${cli_dir}"
printf '#!/bin/sh\nexit 0\n' >"${cli_dir}/redis-server"
printf '#!/bin/sh\necho injected-cli\n' >"${cli_dir}/redis-cli"
chmod +x "${cli_dir}/redis-server" "${cli_dir}/redis-cli"
PATH="/usr/bin:/bin"
export PATH
if command -v redis-cli >/dev/null 2>&1; then
  echo "redis-cli still visible after hiding /usr/local/bin; PATH injection test skipped" >&2
else
  REDIS_SERVER_BIN="${cli_dir}/redis-server"
  require_test_commands redis-cli
  [[ "$(command -v redis-cli)" == "${cli_dir}/redis-cli" ]]
  unset REDIS_SERVER_BIN
fi
PATH="${ORIGINAL_PATH}"
export PATH

echo "platform compatibility helpers passed"
