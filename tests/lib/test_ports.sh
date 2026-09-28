#!/usr/bin/env bash
# Derive collision-free, run-unique test port blocks.
#
# Test runners declare default listen ports inline (for example
# SYNC_SRC_BASE="${SYNC_SRC_BASE:-31100}"). Those defaults are convenient for a
# developer running one category by hand, but they are unsafe when one runner
# executes several categories in sequence: every nightly job, and every category
# inside a job, would contend for the same fixed ports.
#
# Sourcing this file re-bases every declared default onto a private block for
# the current run and job. Blocks sit below 22768 so both the data port and the
# Redis Cluster bus (port+10000) stay under the typical Linux ephemeral floor
# (32768). IANA's 49152-65535 dynamic range overlaps that ephemeral window and
# must not be used for listen sockets.
#
# A hash alone cannot guarantee uniqueness: with ~2000 candidate slots and ~20
# categories per run the birthday problem makes a collision likely. So the
# derivation is backed by a per-run registry file. Each category claims a slot,
# records it, and on collision probes forward to the next free slot. The result
# is unique within a run and stable for a given run id, so a re-run of the same
# case reuses its ports while parallel categories never overlap.
#
# Explicit environment values always win: pass SYNC_SRC_BASE=... to keep a
# hand-picked port, or set TEST_PORT_OFFSET to pin the whole block.
#
# Slot indexes are listen ports, not "one slot per variable". A cluster
# *_BASE occupies BASE..BASE+N-1 (N=3 for masters-only, N=6 with replicas).
# The next BASE or HTTP port must start after that span. Adjacent
# test_port_at indexes are only safe for standalone single-port values.
#
# Usage:
#   source "${ROOT}/tests/lib/test_ports.sh"
#   test_ports_derive <category-name>

# Number of ports reserved per category. The widest runner is a mixed
# cluster-to-standalone pair with replicas: four 6-node bases plus standalone
# and HTTP ports, which needs 32 listen ports after stride packing.
TEST_PORT_SPAN=32
# Highest data port whose default cluster bus (port+10000) is still < 32768.
TEST_PORT_BUS_SAFE_END=22767
# Block start range. Stay unprivileged and keep every data port at or below
# TEST_PORT_BUS_SAFE_END so the derived block itself does not need bus remap.
TEST_PORT_RANGE_START=20000
# Highest usable block start: the whole span must stay bus-safe.
TEST_PORT_RANGE_END=$((TEST_PORT_BUS_SAFE_END - TEST_PORT_SPAN + 1))

test_ports_checksum() {
  # FNV-1a over the seed string, computed in pure bash.
  #
  # This runs once per category per call, and the nightly runner re-resolves a
  # runner's ports several times, so spawning python3 or cksum per call costs
  # real time. A shell hash keeps the derivation deterministic across runs
  # (unlike $RANDOM) with no external process.
  local seed=$1
  local hash=2166136261 index character
  local length=${#seed}
  for ((index = 0; index < length; index++)); do
    printf -v character '%d' "'${seed:index:1}" 2>/dev/null || character=0
    hash=$(((hash ^ character) * 16777619 & 0xffffffff))
  done
  printf '%s' "${hash}"
}

# Registry file holding the "<offset> <name>" slots already claimed in this run.
test_ports_registry_file() {
  printf '%s\n' "${TEST_PORT_REGISTRY:-${TMPDIR:-/tmp}/redis-gunyu-test-ports-${GITHUB_RUN_ID:-local}.tsv}"
}

test_ports_registry_lookup() {
  # Print the offset previously claimed by this exact name, or nothing.
  local name=$1 file offset recorded
  file=$(test_ports_registry_file)
  [[ -f "${file}" ]] || return 1
  while read -r offset recorded; do
    [[ -n "${offset}" ]] || continue
    if [[ "${recorded}" == "${name}" ]]; then
      printf '%s\n' "${offset}"
      return 0
    fi
  done < "${file}"
  return 1
}

test_ports_registry_claimed() {
  # Print the name that owns an offset in this run, or nothing.
  local offset=$1 file recorded name
  file=$(test_ports_registry_file)
  [[ -f "${file}" ]] || return 1
  while read -r recorded name; do
    [[ -n "${recorded}" ]] || continue
    if [[ "${recorded}" == "${offset}" ]]; then
      printf '%s\n' "${name}"
      return 0
    fi
  done < "${file}"
  return 1
}

test_ports_registry_claim() {
  local offset=$1 name=$2 file
  file=$(test_ports_registry_file)
  printf '%s %s\n' "${offset}" "${name}" >> "${file}"
}

# Derive the port block for a category and export TEST_PORT_OFFSET plus the
# TEST_PORT_BLOCK_END guard so every declared base lands inside the block.
test_ports_derive() {
  local name=${1:?test_ports_derive requires a category name}

  # A caller-pinned block is honoured verbatim.
  if [[ -n "${TEST_PORT_OFFSET:-}" ]]; then
    TEST_PORT_BLOCK_END=$((TEST_PORT_OFFSET + TEST_PORT_SPAN - 1))
    export TEST_PORT_BLOCK_END
    export TEST_PORT_BLOCK_NAME="${name}"
    export TEST_PORT_BLOCK_SEED="pinned"
    return 0
  fi

  # Re-deriving for the same category in the same run must not consume a second
  # slot: re-runs of a single case are common and must stay reproducible.
  local existing
  if existing=$(test_ports_registry_lookup "${name}"); then
    TEST_PORT_OFFSET="${existing}"
    TEST_PORT_BLOCK_END=$((TEST_PORT_OFFSET + TEST_PORT_SPAN - 1))
    export TEST_PORT_OFFSET TEST_PORT_BLOCK_END
    export TEST_PORT_BLOCK_NAME="${name}"
    export TEST_PORT_BLOCK_SEED="registry"
    return 0
  fi

  local slots
  slots=$(((TEST_PORT_RANGE_END - TEST_PORT_RANGE_START) / TEST_PORT_SPAN + 1))
  if ((slots < 1)); then
    echo "test port range is too small: start=${TEST_PORT_RANGE_START}" >&2
    return 1
  fi

  # A single run may host many categories (bisync core alone runs six), and
  # several jobs share one repository, so mix run, job, and category name.
  local seed="${GITHUB_RUN_ID:-local}:${GITHUB_JOB:-${GITHUB_WORKFLOW:-${NIGHTLY_SUITE:-default}}}:${name}"
  local checksum start slot offset attempt owner
  checksum=$(test_ports_checksum "${seed}")
  start=$((checksum % slots))

  # Linear probe forward from the hashed slot until an unclaimed one is found.
  offset=""
  for ((attempt = 0; attempt < slots; attempt++)); do
    slot=$(((start + attempt) % slots))
    offset=$((TEST_PORT_RANGE_START + slot * TEST_PORT_SPAN))
    if ! owner=$(test_ports_registry_claimed "${offset}"); then
      break
    fi
    owner=""
    offset=""
  done
  if [[ -z "${offset}" ]]; then
    echo "no free test port block remains in ${TEST_PORT_RANGE_START}-${TEST_PORT_RANGE_END}" >&2
    echo "  registry=$(test_ports_registry_file)" >&2
    return 1
  fi

  test_ports_registry_claim "${offset}" "${name}"
  TEST_PORT_OFFSET="${offset}"
  TEST_PORT_BLOCK_END=$((TEST_PORT_OFFSET + TEST_PORT_SPAN - 1))
  export TEST_PORT_OFFSET TEST_PORT_BLOCK_END
  export TEST_PORT_BLOCK_NAME="${name}"
  export TEST_PORT_BLOCK_SEED="${seed}"
}

# Fail loudly when a declared base sits outside the derived block. A base that
# is off-block is either a stale default we failed to re-base or a hardcoded
# collision the derivation cannot fix; either way the run must stop here rather
# than flake later.
test_ports_assert_declared() {
  local name=$1
  shift
  if [[ -z "${TEST_PORT_BLOCK_END:-}" ]]; then
    return 0
  fi
  local base
  for base in "$@"; do
    [[ -n "${base}" ]] || continue
    if ((base < TEST_PORT_OFFSET || base > TEST_PORT_BLOCK_END)); then
      echo "test port base ${base} is outside the derived block" >&2
      echo "  category=${name} block=${TEST_PORT_OFFSET}-${TEST_PORT_BLOCK_END}" >&2
      echo "  seed=${TEST_PORT_BLOCK_SEED:-unknown}" >&2
      echo "hint: declare the default as \$((TEST_PORT_OFFSET + n)) so it is re-based" >&2
      return 1
    fi
  done
}

# Re-base a declared default onto the current block.
#   SYNC_SRC_BASE="${SYNC_SRC_BASE:-$(test_port_at 0)}"
test_port_at() {
  local index=${1:?test_port_at requires a slot index}
  if [[ -z "${TEST_PORT_OFFSET:-}" ]]; then
    echo "test_port_at requires test_ports_derive to run first" >&2
    return 1
  fi
  printf '%s\n' "$((TEST_PORT_OFFSET + index))"
}

# Refuse to run when the derived block is already occupied, or when it overlaps
# a live cluster bus port. Callers treat a failure as a hard error: the block is
# run-unique, so an occupied port means a stale process or a genuine conflict
# that must be reported rather than papered over.
test_ports_require_free() {
  local name=${1:?test_ports_require_free requires a category name}
  shift
  local base offset port bus
  for base in "$@"; do
    [[ -n "${base}" ]] || continue
    for offset in 0 1 2 3 4 5; do
      port=$((base + offset))
      if port_is_open "${port}"; then
        echo "test port ${port} is already occupied" >&2
        echo "  category=${name} block=${TEST_PORT_OFFSET}-${TEST_PORT_BLOCK_END}" >&2
        echo "  seed=${TEST_PORT_BLOCK_SEED:-unknown}" >&2
        echo "hint: re-run with TEST_PORT_OFFSET=<start> to move the block" >&2
        return 1
      fi
      bus=$(cluster_bus_port "${port}")
      if port_is_open "${bus}"; then
        echo "cluster bus candidate ${bus} is already occupied" >&2
        echo "  category=${name} block=${TEST_PORT_OFFSET}-${TEST_PORT_BLOCK_END}" >&2
        echo "  seed=${TEST_PORT_BLOCK_SEED:-unknown}" >&2
        return 1
      fi
    done
  done
}
