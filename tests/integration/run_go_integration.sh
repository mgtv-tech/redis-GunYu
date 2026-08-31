#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/nonbisync/lib/test_env.sh"
require_test_commands

RUN_ID="${TEST_RUN_ID:-$(date -u '+%Y%m%dT%H%M%SZ')-$$}"
ARTIFACT_BASE="${ARTIFACT_ROOT:-${ROOT}/.artifacts/tests/go-integration}"
ARTIFACT_ROOT="${ARTIFACT_BASE%/}/${RUN_ID}"
REDIS_ROOT="${ARTIFACT_ROOT}/redis"
RESULT_ROOT="${ARTIFACT_ROOT}/results"
REDIS_SERVER_BIN="$(resolve_redis_server_bin REDIS_SERVER_BIN REDIS_DEPLOY_ROOT)"
KEEP_REDIS_FILES="${KEEP_REDIS_FILES:-1}"
INTEGRATION_PORT_BASE="${INTEGRATION_PORT_BASE:-}"
PORTS=()

mkdir -p "${REDIS_ROOT}" "${RESULT_ROOT}"

port_is_open() {
  local port=$1
  (echo >/dev/tcp/127.0.0.1/"${port}") >/dev/null 2>&1
}

candidate_is_free() {
  local base=$1
  local offset port
  for offset in 0 1 2 3 4 5 6; do
    port=$((base + offset))
    if port_is_open "${port}" || port_is_open "$((port + 10000))"; then
      return 1
    fi
  done
}

choose_port_base() {
  local candidate attempt
  if [[ -n "${INTEGRATION_PORT_BASE}" ]]; then
    candidate=${INTEGRATION_PORT_BASE}
    if ! candidate_is_free "${candidate}"; then
      echo "integration port block is occupied: ${candidate}-$((candidate + 6))" >&2
      return 1
    fi
    printf '%s\n' "${candidate}"
    return 0
  fi

  for attempt in $(seq 1 30); do
    candidate=$((22000 + RANDOM % 18000))
    if candidate_is_free "${candidate}"; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done
  echo "could not find a free Redis port block" >&2
  return 1
}

stop_owned_redis() {
  local pid_file pid
  while IFS= read -r pid_file; do
    [[ -f "${pid_file}" ]] || continue
    pid=$(tr -d '[:space:]' < "${pid_file}")
    if [[ "${pid}" =~ ^[0-9]+$ ]] && kill -0 "${pid}" >/dev/null 2>&1; then
      kill "${pid}" >/dev/null 2>&1 || true
    fi
  done < <(find "${REDIS_ROOT}" -type f -name redis.pid 2>/dev/null)
}

cleanup() {
  local code=$?
  set +e
  stop_owned_redis
  if [[ "${KEEP_REDIS_FILES}" != "1" ]]; then
    if [[ -n "${REDIS_ROOT}" && "${REDIS_ROOT}" == "${ARTIFACT_ROOT}/redis" && -d "${REDIS_ROOT}" ]]; then
      rm -rf -- "${REDIS_ROOT}"
    fi
  fi
  exit "${code}"
}
trap cleanup EXIT

PORT_BASE="$(choose_port_base)"
STANDALONE_PORT=${PORT_BASE}
CLUSTER_PORTS=("$((PORT_BASE + 1))" "$((PORT_BASE + 2))" "$((PORT_BASE + 3))" "$((PORT_BASE + 4))" "$((PORT_BASE + 5))" "$((PORT_BASE + 6))")
PORTS=("${STANDALONE_PORT}" "${CLUSTER_PORTS[@]}")

start_standalone "${REDIS_SERVER_BIN}" "${REDIS_ROOT}" standalone "${STANDALONE_PORT}"
start_cluster_with_replicas "${REDIS_SERVER_BIN}" "${REDIS_ROOT}" cluster "${CLUSTER_PORTS[@]}"

MASTER_ADDR=""
REPLICA_ADDR=""
for port in "${CLUSTER_PORTS[@]}"; do
  role=$(redis-cli -p "${port}" --raw role | head -n 1)
  if [[ "${role}" == "master" && -z "${MASTER_ADDR}" ]]; then
    MASTER_ADDR="127.0.0.1:${port}"
  elif [[ "${role}" == "slave" && -z "${REPLICA_ADDR}" ]]; then
    REPLICA_ADDR="127.0.0.1:${port}"
  fi
done
if [[ -z "${MASTER_ADDR}" || -z "${REPLICA_ADDR}" ]]; then
  echo "failed to discover cluster master and replica" >&2
  exit 1
fi

REDIS_VERSION=$("${REDIS_SERVER_BIN}" --version | sed -n 's/.* v=\([^ ]*\).*/\1/p')
REDIS_VERSION=${REDIS_VERSION:-unknown}

{
  echo "run_id=${RUN_ID}"
  echo "git_commit=$(git -C "${ROOT}" rev-parse HEAD)"
  echo "git_dirty=$([[ -n "$(git -C "${ROOT}" status --porcelain)" ]] && echo true || echo false)"
  echo "go_version=$(go version)"
  echo "redis_server=${REDIS_SERVER_BIN}"
  echo "redis_version=${REDIS_VERSION}"
  echo "standalone=127.0.0.1:${STANDALONE_PORT}"
  echo "cluster_seed=127.0.0.1:${CLUSTER_PORTS[0]}"
  echo "cluster_master=${MASTER_ADDR}"
  echo "cluster_replica=${REPLICA_ADDR}"
} > "${ARTIFACT_ROOT}/manifest.txt"

redis-cli -p "${STANDALONE_PORT}" info server > "${REDIS_ROOT}/standalone-info.txt"
redis-cli -p "${STANDALONE_PORT}" role > "${REDIS_ROOT}/standalone-role.txt"
redis-cli -p "${CLUSTER_PORTS[0]}" cluster info > "${REDIS_ROOT}/cluster-info.txt"
redis-cli -p "${CLUSTER_PORTS[0]}" cluster nodes > "${REDIS_ROOT}/cluster-nodes.txt"

PACKAGES=(
  ./cmd
  ./pkg/cluster
  ./pkg/redis
	./pkg/redis/client
  ./pkg/redis/client/cluster
  ./pkg/redis/client/conn
  ./syncer
)

set +e
(
  cd "${ROOT}"
  REQUIRE_REDIS_INTEGRATION=1 \
	REDIS_SERVER_BIN="${REDIS_SERVER_BIN}" \
  TEST_REDIS_ADDR="127.0.0.1:${STANDALONE_PORT}" \
  TEST_REDIS_CLUSTER_ADDR="127.0.0.1:${CLUSTER_PORTS[0]}" \
  TEST_REDIS_CLUSTER_MASTER_ADDR="${MASTER_ADDR}" \
  TEST_REDIS_CLUSTER_REPLICA_ADDR="${REPLICA_ADDR}" \
  TEST_REDIS_STANDALONE_ADDR="127.0.0.1:${STANDALONE_PORT}" \
  TEST_REDIS_CLUSTER_VERSION="${REDIS_VERSION}" \
  go test -json -tags=integration -count=1 "${PACKAGES[@]}"
) > "${RESULT_ROOT}/go-test.jsonl"
GO_TEST_STATUS=$?
set -e

GATE_ARGS=(
  -input "${RESULT_ROOT}/go-test.jsonl"
  -json-output "${RESULT_ROOT}/gate.json"
  -markdown-output "${RESULT_ROOT}/summary.md"
  -require-test TestMigration
  -require-test TestRegistry
  -require-test TestCampaign
  -require-test TestGetRedisRoleOnlineRealCluster
	-require-test TestSentinelDiscoveryFallbackAuthenticationAndFailover
  -require-test TestCheckRepliesWithRealRedis
)
for package in "${PACKAGES[@]}"; do
  package=${package#./}
  GATE_ARGS+=( -require-package "github.com/mgtv-tech/redis-GunYu/${package}" )
done

set +e
(cd "${ROOT}" && go run ./tests/cmd/testjson_gate "${GATE_ARGS[@]}")
GATE_STATUS=$?
set -e

echo "artifact_root=${ARTIFACT_ROOT}"
if [[ ${GO_TEST_STATUS} -ne 0 || ${GATE_STATUS} -ne 0 ]]; then
  exit 1
fi
