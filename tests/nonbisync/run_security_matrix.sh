#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_ROOT="${TMPDIR:-/tmp}/redisgunyu-nonbisync-security"
source "${ROOT}/tests/nonbisync/lib/test_env.sh"

AUTH_PASSWORD="${AUTH_PASSWORD:-nonbisync-pass}"
AUTH_USER="${AUTH_USER:-}"
ENABLE_TLS="${ENABLE_TLS:-0}"
TLS_SRC_PORT="${TLS_SRC_PORT:-36300}"
TLS_DST_PORT="${TLS_DST_PORT:-36400}"
TLS_HTTP_PORT="${TLS_HTTP_PORT:-36380}"
AUTH_STD_SRC_PORT="${AUTH_STD_SRC_PORT:-35900}"
AUTH_STD_DST_PORT="${AUTH_STD_DST_PORT:-36000}"
AUTH_STD_HTTP_PORT="${AUTH_STD_HTTP_PORT:-35980}"
AUTH_CLUSTER_SRC_BASE="${AUTH_CLUSTER_SRC_BASE:-36100}"
AUTH_CLUSTER_DST_BASE="${AUTH_CLUSTER_DST_BASE:-36200}"
AUTH_CLUSTER_HTTP_PORT="${AUTH_CLUSTER_HTTP_PORT:-36180}"
TEST_PREFIX="${TEST_PREFIX:-nonbisync:security:$(date +%s)}"
SYNCER_PID=""
REDIS_SERVER_BIN="$(resolve_redis_server_bin REDIS_SERVER_BIN REDIS_DEPLOY_ROOT)"

cleanup() {
  local code=$?
  set +e
  stop_pid "${SYNCER_PID:-}"
  shutdown_ports \
    "${AUTH_STD_SRC_PORT}" "${AUTH_STD_DST_PORT}" \
    "${AUTH_CLUSTER_SRC_BASE}" "$((AUTH_CLUSTER_SRC_BASE + 1))" "$((AUTH_CLUSTER_SRC_BASE + 2))" \
    "${AUTH_CLUSTER_DST_BASE}" "$((AUTH_CLUSTER_DST_BASE + 1))" "$((AUTH_CLUSTER_DST_BASE + 2))" \
    "${TLS_SRC_PORT}" "${TLS_DST_PORT}"
  if [[ "${KEEP_TMP:-0}" != "1" ]]; then
    rm -rf "${TMP_ROOT}"
  fi
  exit "${code}"
}
trap cleanup EXIT

rm -rf "${TMP_ROOT}"
mkdir -p "${TMP_ROOT}"

wait_for_ping_auth() {
  local port=$1
  for _ in $(seq 1 50); do
    if redis-cli -a "${AUTH_PASSWORD}" -p "${port}" ping >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  echo "auth redis on port ${port} did not start" >&2
  return 1
}

wait_for_cluster_ok_auth() {
  local port=$1
  for _ in $(seq 1 100); do
    if redis-cli -a "${AUTH_PASSWORD}" -p "${port}" cluster info 2>/dev/null | rg -q '^cluster_state:ok'; then
      return 0
    fi
    sleep 0.2
  done
  echo "auth cluster on port ${port} did not become ready" >&2
  return 1
}

start_auth_standalone() {
  local prefix=$1
  local port=$2
  write_standalone_conf "${TMP_ROOT}/${prefix}-${port}" "${port}" "requirepass ${AUTH_PASSWORD}"
  "${REDIS_SERVER_BIN}" "${TMP_ROOT}/${prefix}-${port}/redis.conf"
  wait_for_ping_auth "${port}"
}

start_auth_cluster() {
  local prefix=$1
  shift
  local ports=("$@")
  local port
  local extra_conf
  extra_conf=$'requirepass '"${AUTH_PASSWORD}"$'\nmasterauth '"${AUTH_PASSWORD}"
  for port in "${ports[@]}"; do
    write_cluster_conf "${TMP_ROOT}/${prefix}-${port}" "${port}" "${extra_conf}"
    "${REDIS_SERVER_BIN}" "${TMP_ROOT}/${prefix}-${port}/redis.conf"
    wait_for_ping_auth "${port}"
  done
  redis-cli -a "${AUTH_PASSWORD}" --cluster create \
    $(printf '127.0.0.1:%s ' "${ports[@]}") \
    --cluster-replicas 0 \
    --cluster-yes >/dev/null
  for port in "${ports[@]}"; do
    wait_for_cluster_ok_auth "${port}"
  done
}

write_security_conf() {
  local file=$1
  local http_port=$2
  local input_addrs=$3
  local input_type=$4
  local output_addrs=$5
  local output_type=$6
  local storer_dir=$7
  local replay_mode=$8
  local tls_enable=${9:-false}
  local redis_password=${10:-$AUTH_PASSWORD}

  mkdir -p "${storer_dir}"
  cat > "${file}" <<EOF
server:
  listen: 127.0.0.1:${http_port}
  listenPeer: 127.0.0.1:${http_port}
  gracefullStopTimeout: 1s
  checkRedisTypologyTicker: 2s
input:
  redis:
    addresses: [${input_addrs}]
    type: ${input_type}
    version: "7.0.11"
    password: ${redis_password}
    tlsEnable: ${tls_enable}
  mode: dynamic
  syncFrom: master
channel:
  storer:
    dirPath: ${storer_dir}
    maxSize: 104857600
    logSize: 10485760
  staleCheckpointDuration: 10m
output:
  redis:
    addresses: [${output_addrs}]
    type: ${output_type}
    version: "7.0.11"
    password: ${redis_password}
    tlsEnable: ${tls_enable}
  replay:
    resumeFromBreakPoint: true
    keyExists: replace
    metric: false
    targetDb: -1
    replayTransaction: true
    bisyncEnabled: false
    mode: ${replay_mode}
log:
  level: info
  handler:
    stdout: true
  withCaller: false
  withFunc: false
EOF
}

wait_for_value_auth() {
  local port=$1
  local key=$2
  local expected=$3
  for _ in $(seq 1 80); do
    if [[ "$(redis-cli -a "${AUTH_PASSWORD}" -p "${port}" --raw get "${key}" 2>/dev/null || true)" == "${expected}" ]]; then
      return 0
    fi
    sleep 0.25
  done
  echo "expected auth key ${key} to become ${expected}" >&2
  return 1
}

scan_count_auth_standalone() {
  local port=$1
  local pattern=$2
  redis-cli -a "${AUTH_PASSWORD}" -p "${port}" --scan --pattern "${pattern}" 2>/dev/null | wc -l | tr -d ' '
}

scan_count_auth_cluster() {
  local pattern=$1
  shift
  local port
  for port in "$@"; do
    redis-cli -a "${AUTH_PASSWORD}" -p "${port}" --scan --pattern "${pattern}" 2>/dev/null || true
  done | sort -u | sed '/^$/d' | wc -l | tr -d ' '
}

assert_no_bisync_metadata_auth_standalone() {
  local port=$1
  local marker_count latest_count commit_count frontier_count
  marker_count=$(scan_count_auth_standalone "${port}" 'redis-gunyu-bisync:*:marker:*')
  latest_count=$(scan_count_auth_standalone "${port}" 'redis-gunyu-bisync:*:latest:*')
  commit_count=$(scan_count_auth_standalone "${port}" 'redis-gunyu-bisync:*:commit:*')
  frontier_count=$(scan_count_auth_standalone "${port}" 'redis-gunyu-bisync:*:frontier')
  if [[ "${marker_count}" != "0" || "${latest_count}" != "0" || "${commit_count}" != "0" || "${frontier_count}" != "0" ]]; then
    echo "expected no bisync metadata on auth standalone target, got marker=${marker_count} latest=${latest_count} commit=${commit_count} frontier=${frontier_count}" >&2
    exit 1
  fi
}

assert_no_bisync_metadata_auth_cluster() {
  local marker_count latest_count commit_count frontier_count
  marker_count=$(scan_count_auth_cluster 'redis-gunyu-bisync:*:marker:*' "$@")
  latest_count=$(scan_count_auth_cluster 'redis-gunyu-bisync:*:latest:*' "$@")
  commit_count=$(scan_count_auth_cluster 'redis-gunyu-bisync:*:commit:*' "$@")
  frontier_count=$(scan_count_auth_cluster 'redis-gunyu-bisync:*:frontier' "$@")
  if [[ "${marker_count}" != "0" || "${latest_count}" != "0" || "${commit_count}" != "0" || "${frontier_count}" != "0" ]]; then
    echo "expected no bisync metadata on auth cluster target, got marker=${marker_count} latest=${latest_count} commit=${commit_count} frontier=${frontier_count}" >&2
    exit 1
  fi
}

assert_checkpoint_signals_auth_standalone() {
  local port=$1
  local hash_len key_count
  hash_len=$(redis-cli -a "${AUTH_PASSWORD}" -p "${port}" --raw hlen redis-gunyu-checkpoint-hash 2>/dev/null || echo 0)
  key_count=$(scan_count_auth_standalone "${port}" 'redis-gunyu-checkpoint*')
  if [[ "${hash_len}" == "0" || "${key_count}" == "0" ]]; then
    echo "expected checkpoint signals on auth standalone target, got hash_len=${hash_len} key_count=${key_count}" >&2
    exit 1
  fi
}

assert_checkpoint_signals_auth_cluster() {
  local port=$1
  shift
  local hash_len key_count
  hash_len=$(redis-cli -a "${AUTH_PASSWORD}" -c -p "${port}" --raw hlen redis-gunyu-checkpoint-hash 2>/dev/null || echo 0)
  key_count=$(scan_count_auth_cluster 'redis-gunyu-checkpoint*' "${port}" "$@")
  if [[ "${hash_len}" == "0" || "${key_count}" == "0" ]]; then
    echo "expected checkpoint signals on auth cluster target, got hash_len=${hash_len} key_count=${key_count}" >&2
    exit 1
  fi
}

scan_count_tls_standalone() {
  local port=$1
  local ca_cert=$2
  local pattern=$3
  redis-cli --tls --cacert "${ca_cert}" -p "${port}" --scan --pattern "${pattern}" 2>/dev/null | wc -l | tr -d ' '
}

assert_no_bisync_metadata_tls_standalone() {
  local port=$1
  local ca_cert=$2
  local marker_count latest_count commit_count frontier_count
  marker_count=$(scan_count_tls_standalone "${port}" "${ca_cert}" 'redis-gunyu-bisync:*:marker:*')
  latest_count=$(scan_count_tls_standalone "${port}" "${ca_cert}" 'redis-gunyu-bisync:*:latest:*')
  commit_count=$(scan_count_tls_standalone "${port}" "${ca_cert}" 'redis-gunyu-bisync:*:commit:*')
  frontier_count=$(scan_count_tls_standalone "${port}" "${ca_cert}" 'redis-gunyu-bisync:*:frontier')
  if [[ "${marker_count}" != "0" || "${latest_count}" != "0" || "${commit_count}" != "0" || "${frontier_count}" != "0" ]]; then
    echo "expected no bisync metadata on tls standalone target, got marker=${marker_count} latest=${latest_count} commit=${commit_count} frontier=${frontier_count}" >&2
    exit 1
  fi
}

assert_checkpoint_signals_tls_standalone() {
  local port=$1
  local ca_cert=$2
  local hash_len key_count
  hash_len=$(redis-cli --tls --cacert "${ca_cert}" -p "${port}" --raw hlen redis-gunyu-checkpoint-hash 2>/dev/null || echo 0)
  key_count=$(scan_count_tls_standalone "${port}" "${ca_cert}" 'redis-gunyu-checkpoint*')
  if [[ "${hash_len}" == "0" || "${key_count}" == "0" ]]; then
    echo "expected checkpoint signals on tls standalone target, got hash_len=${hash_len} key_count=${key_count}" >&2
    exit 1
  fi
}

run_auth_standalone() {
  local prefix="${TEST_PREFIX}:auth-standalone"
  echo "[security] auth standalone"
  start_auth_standalone src "${AUTH_STD_SRC_PORT}"
  start_auth_standalone dst "${AUTH_STD_DST_PORT}"

  redis-cli -a "${AUTH_PASSWORD}" -p "${AUTH_STD_SRC_PORT}" set "${prefix}:string" "auth-standalone" >/dev/null

  write_security_conf "${TMP_ROOT}/auth-standalone.yaml" "${AUTH_STD_HTTP_PORT}" "\"127.0.0.1:${AUTH_STD_SRC_PORT}\"" standalone "\"127.0.0.1:${AUTH_STD_DST_PORT}\"" standalone "${TMP_ROOT}/auth-standalone-store" sync false "${AUTH_PASSWORD}"
  SYNCER_PID=$(start_syncer_process "${TMP_ROOT}" "${TMP_ROOT}/auth-standalone.yaml" "${TMP_ROOT}/auth-standalone.log")
  wait_for_syncer "${AUTH_STD_HTTP_PORT}"
  wait_for_value_auth "${AUTH_STD_DST_PORT}" "${prefix}:string" "auth-standalone"
  redis-cli -a "${AUTH_PASSWORD}" -p "${AUTH_STD_DST_PORT}" set "${prefix}:isolated" "target-only" >/dev/null
  expect_absent "$(redis-cli -a "${AUTH_PASSWORD}" -p "${AUTH_STD_SRC_PORT}" --raw exists "${prefix}:isolated" 2>/dev/null || echo 0)" "auth standalone isolated target key on source"
  assert_no_bisync_metadata_auth_standalone "${AUTH_STD_DST_PORT}"
  assert_checkpoint_signals_auth_standalone "${AUTH_STD_DST_PORT}"
  assert_log_has_no_bisync_markers "${TMP_ROOT}/auth-standalone.log"
  stop_pid "${SYNCER_PID}"
  SYNCER_PID=""
}

run_auth_cluster() {
  local prefix="${TEST_PREFIX}:auth-cluster"
  local src_ports=("${AUTH_CLUSTER_SRC_BASE}" "$((AUTH_CLUSTER_SRC_BASE + 1))" "$((AUTH_CLUSTER_SRC_BASE + 2))")
  local dst_ports=("${AUTH_CLUSTER_DST_BASE}" "$((AUTH_CLUSTER_DST_BASE + 1))" "$((AUTH_CLUSTER_DST_BASE + 2))")
  local src_csv dst_csv target_port source_port
  echo "[security] auth cluster"
  start_auth_cluster src "${src_ports[@]}"
  start_auth_cluster dst "${dst_ports[@]}"
  src_csv=$(format_addrs "${src_ports[@]}")
  dst_csv=$(format_addrs "${dst_ports[@]}")

  redis-cli -a "${AUTH_PASSWORD}" -c -p "${src_ports[0]}" set "${prefix}:string" "auth-cluster" >/dev/null

  write_security_conf "${TMP_ROOT}/auth-cluster.yaml" "${AUTH_CLUSTER_HTTP_PORT}" "${src_csv}" cluster "${dst_csv}" cluster "${TMP_ROOT}/auth-cluster-store" sync false "${AUTH_PASSWORD}"
  SYNCER_PID=$(start_syncer_process "${TMP_ROOT}" "${TMP_ROOT}/auth-cluster.yaml" "${TMP_ROOT}/auth-cluster.log")
  wait_for_syncer "${AUTH_CLUSTER_HTTP_PORT}"
  for _ in $(seq 1 80); do
    if "${TMP_ROOT}/bisync_compare" --left-addrs "${src_csv}" --left-password "${AUTH_PASSWORD}" --right-addrs "${dst_csv}" --right-password "${AUTH_PASSWORD}" --pattern "${prefix}:*" >/dev/null 2>&1; then
      target_port=${dst_ports[0]}
      source_port=${src_ports[0]}
      redis-cli -a "${AUTH_PASSWORD}" -c -p "${target_port}" set "${prefix}:isolated" "target-only" >/dev/null
      expect_absent "$(redis-cli -a "${AUTH_PASSWORD}" -c -p "${source_port}" --raw exists "${prefix}:isolated" 2>/dev/null || echo 0)" "auth cluster isolated target key on source"
      assert_no_bisync_metadata_auth_cluster "${dst_ports[@]}"
      assert_checkpoint_signals_auth_cluster "${target_port}" "${dst_ports[@]}"
      assert_log_has_no_bisync_markers "${TMP_ROOT}/auth-cluster.log"
      stop_pid "${SYNCER_PID}"
      SYNCER_PID=""
      return 0
    fi
    sleep 0.25
  done
  echo "auth cluster compare failed" >&2
  return 1
}

wait_for_ping_tls() {
  local port=$1
  local ca_cert=$2
  for _ in $(seq 1 60); do
    if redis-cli --tls --cacert "${ca_cert}" -p "${port}" ping >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  echo "tls redis on port ${port} did not start" >&2
  return 1
}

run_tls_standalone() {
  local cert_dir="${TMP_ROOT}/tls"
  local prefix="${TEST_PREFIX}:tls-standalone"
  if [[ "${ENABLE_TLS}" != "1" ]]; then
    return 0
  fi
  if ! command -v openssl >/dev/null 2>&1; then
    echo "openssl not found; skipping tls standalone test" >&2
    return 0
  fi
  if ! "${REDIS_SERVER_BIN}" --help 2>&1 | rg -q 'tls-port'; then
    echo "redis-server has no TLS support; skipping tls standalone test" >&2
    return 0
  fi

  echo "[security] tls standalone"
  mkdir -p "${cert_dir}"
  openssl req -x509 -newkey rsa:2048 -sha256 -days 1 -nodes \
    -keyout "${cert_dir}/ca.key" -out "${cert_dir}/ca.crt" -subj "/CN=nonbisync-ca" >/dev/null 2>&1
  openssl req -newkey rsa:2048 -nodes \
    -keyout "${cert_dir}/server.key" -out "${cert_dir}/server.csr" -subj "/CN=127.0.0.1" >/dev/null 2>&1
  openssl x509 -req -in "${cert_dir}/server.csr" -CA "${cert_dir}/ca.crt" -CAkey "${cert_dir}/ca.key" -CAcreateserial \
    -out "${cert_dir}/server.crt" -days 1 -sha256 \
    -extfile <(printf 'subjectAltName=IP:127.0.0.1\n') >/dev/null 2>&1

  local tls_extra
  tls_extra=$'port 0\n'"tls-port ${TLS_SRC_PORT}"$'\n'"tls-cert-file ${cert_dir}/server.crt"$'\n'"tls-key-file ${cert_dir}/server.key"$'\n'"tls-ca-cert-file ${cert_dir}/ca.crt"$'\n'"tls-auth-clients no"
  write_standalone_conf "${TMP_ROOT}/tls-src-${TLS_SRC_PORT}" "${TLS_SRC_PORT}" "${tls_extra}"
  "${REDIS_SERVER_BIN}" "${TMP_ROOT}/tls-src-${TLS_SRC_PORT}/redis.conf"
  tls_extra=$'port 0\n'"tls-port ${TLS_DST_PORT}"$'\n'"tls-cert-file ${cert_dir}/server.crt"$'\n'"tls-key-file ${cert_dir}/server.key"$'\n'"tls-ca-cert-file ${cert_dir}/ca.crt"$'\n'"tls-auth-clients no"
  write_standalone_conf "${TMP_ROOT}/tls-dst-${TLS_DST_PORT}" "${TLS_DST_PORT}" "${tls_extra}"
  "${REDIS_SERVER_BIN}" "${TMP_ROOT}/tls-dst-${TLS_DST_PORT}/redis.conf"
  wait_for_ping_tls "${TLS_SRC_PORT}" "${cert_dir}/ca.crt"
  wait_for_ping_tls "${TLS_DST_PORT}" "${cert_dir}/ca.crt"

  redis-cli --tls --cacert "${cert_dir}/ca.crt" -p "${TLS_SRC_PORT}" set "${prefix}:string" "tls-standalone" >/dev/null

  write_security_conf "${TMP_ROOT}/tls-standalone.yaml" "${TLS_HTTP_PORT}" "\"127.0.0.1:${TLS_SRC_PORT}\"" standalone "\"127.0.0.1:${TLS_DST_PORT}\"" standalone "${TMP_ROOT}/tls-standalone-store" sync true ""
  SYNCER_PID=$(start_syncer_process "${TMP_ROOT}" "${TMP_ROOT}/tls-standalone.yaml" "${TMP_ROOT}/tls-standalone.log")
  wait_for_syncer "${TLS_HTTP_PORT}"
  for _ in $(seq 1 80); do
    if [[ "$(redis-cli --tls --cacert "${cert_dir}/ca.crt" -p "${TLS_DST_PORT}" --raw get "${prefix}:string" 2>/dev/null || true)" == "tls-standalone" ]]; then
      redis-cli --tls --cacert "${cert_dir}/ca.crt" -p "${TLS_DST_PORT}" set "${prefix}:isolated" "target-only" >/dev/null
      expect_absent "$(redis-cli --tls --cacert "${cert_dir}/ca.crt" -p "${TLS_SRC_PORT}" --raw exists "${prefix}:isolated" 2>/dev/null || echo 0)" "tls standalone isolated target key on source"
      assert_no_bisync_metadata_tls_standalone "${TLS_DST_PORT}" "${cert_dir}/ca.crt"
      assert_checkpoint_signals_tls_standalone "${TLS_DST_PORT}" "${cert_dir}/ca.crt"
      assert_log_has_no_bisync_markers "${TMP_ROOT}/tls-standalone.log"
      stop_pid "${SYNCER_PID}"
      SYNCER_PID=""
      return 0
    fi
    sleep 0.25
  done
  echo "tls standalone value did not converge" >&2
  return 1
}

echo "[1/1] building binaries"
build_nonbisync_binaries "${TMP_ROOT}"
run_auth_standalone
run_auth_cluster
run_tls_standalone
