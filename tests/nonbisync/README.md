# Non-Bisync Tests

`tests/nonbisync` contains executable end-to-end checks for the original
single-direction sync path with `output.replay.bisyncEnabled: false`.

The goal is regression coverage for non-bisync behavior after bisync-related
changes.

Current scripts:

- `run_category1.sh`
  Runs single-direction cluster sync smoke tests in `sync` and `pipeline`
  modes. It starts temporary source and target Redis clusters, launches one
  GunYu syncer, writes a mixed source-side workload, verifies exact business
  state on the target, confirms cluster equality for the test prefix, and
  asserts that no bisync metadata namespace is created.

- `run_category2.sh`
  Runs single-direction cluster restart and resume checks in `sync` and
  `pipeline` modes. It writes data, waits for convergence, stops the syncer,
  writes more source-side data while replication is offline, restarts the
  syncer, and verifies exact final business state, checkpoint signals, and
  absence of bisync metadata.

- `run_category3.sh`
  Runs single-direction standalone regression checks in `sync` and `pipeline`
  modes against temporary local Redis instances. It validates steady-state
  convergence, offline-write resume, checkpoint persistence, and one-way sync
  semantics without relying on cluster routing.

- `run_category4.sh`
  Runs single-direction cluster failover and topology-disturbance checks in
  `sync` and `pipeline` modes. It starts source and target Redis clusters with
  replicas, injects source failover, target failover, and syncer restart, then
  verifies final business convergence and the absence of bisync control-plane
  state.

- `run_category5.sh`
  Runs single-direction cluster hotspot-key recovery checks in `sync` and
  `pipeline` modes. It chains source failover, target failover, syncer
  stop/restart, and immediate post-restart writes against the same key set to
  catch dropped updates during topology-recovery windows.

- `run_category6.sh`
  Runs mixed-topology regression checks in `sync` and `pipeline` modes. It
  covers `cluster -> standalone` with source failover plus offline resume, and
  `standalone -> cluster` with target failover plus offline resume.

- `run_category7.sh`
  Runs configuration and filter matrix checks. It covers RDB `keyExists`
  behavior (`replace`, `ignore`, `error`), `targetDbMap`, `dbBlacklist`,
  prefix filters, and cluster slot filters.

- `run_category8.sh`
  Runs rich workload regression checks in `sync` and `pipeline` modes. It
  verifies replay of binary strings, long keys, large payloads, streams, TTL
  boundaries, script-generated writes, and same-slot transactions.

- `run_category9.sh`
  Runs single-direction soak checks with segmented source workloads, source
  failover, target failover, syncer restart, syncer offline catch-up, and
  resource sampling. Stable business keys must still converge exactly without
  depending on force-failover during active writes.

- `run_category10.sh`
  Runs multi-syncer control-plane HA checks with a cluster source and
  standalone target. Two non-bisync syncers share one Redis-based control
  plane, one leader process is terminated, and the follower process must take
  over and continue single-direction replication.

- `run_category11.sh`
  Runs Redis Modules incremental replay checks against temporary Redis Stack
  source and destination instances. It validates standalone single-direction
  replay of RedisJSON, RedisBloom, and RediSearch commands with
  `bisyncEnabled: false`. The runner sets `moduleAuxPolicy: skip` so Redis
  Stack's global module metadata does not block the initial full sync before
  incremental module commands are checked.

- `run_all.sh`
  Executes category1 through category10 sequentially and writes a Markdown
  report with case status, duration, and tail logs.

- `run_security_matrix.sh`
  Optional auth/TLS runner. It validates password-authenticated standalone and
  cluster sync, and when `ENABLE_TLS=1` plus TLS-capable `redis-server` are
  available, it also validates standalone TLS replay.

- `run_version_matrix.sh`
  Optional version-matrix wrapper. It reruns one or more non-bisync runners
  against every `redis-server` binary listed in `REDIS_SERVER_BINS`.

- `run_controlplane_etcd.sh`
  Optional etcd-backed control-plane HA runner. It starts a temporary etcd,
  launches two syncers using `cluster.metaEtcd`, and verifies leader handover.
  It is disabled by default; set `ENABLE_ETCD_TESTS=1` to run it.

Shared helpers live under `tests/nonbisync/lib/` and intentionally reuse
`tests/bisync/lib/redis_env.sh` for Redis binary resolution.

Examples:

```bash
bash ./tests/nonbisync/run_category1.sh
SCENARIOS=sync bash ./tests/nonbisync/run_category2.sh
SCENARIOS=pipeline bash ./tests/nonbisync/run_category3.sh
SCENARIOS=sync KEEP_TMP=1 bash ./tests/nonbisync/run_category4.sh
SCENARIOS=sync KEEP_TMP=1 bash ./tests/nonbisync/run_category5.sh
SCENARIOS=sync bash ./tests/nonbisync/run_category6.sh
SCENARIOS=sync bash ./tests/nonbisync/run_category7.sh
SCENARIOS=sync bash ./tests/nonbisync/run_category8.sh
SOAK_DURATION_SECONDS=120 SCENARIOS=sync bash ./tests/nonbisync/run_category9.sh
SCENARIOS=sync bash ./tests/nonbisync/run_category10.sh
AUTH_PASSWORD=secret bash ./tests/nonbisync/run_security_matrix.sh
REDIS_SERVER_BINS=/path/to/redis-7.0,/path/to/redis-7.2 bash ./tests/nonbisync/run_version_matrix.sh
bash ./tests/nonbisync/run_all.sh
```

Binary discovery:

- All category scripts accept either `REDIS_SERVER_BIN=/path/to/redis-server`
  or `REDIS_DEPLOY_ROOT=/path/to/redis-deploy`.

Current scope:

- Cluster single-direction sync
- Cluster restart and failover recovery
- Hotspot-key replay across failover and restart windows
- Standalone single-direction sync and resume
- Mixed topology replay (`cluster -> standalone`, `standalone -> cluster`)
- RDB `keyExists` and output filter matrix
- Rich command/data-type boundaries including streams and TTL edges
- Redis Modules incremental replay on standalone Redis Stack instances
- Single-direction soak with restart/offline windows and resource sampling
- Redis control-plane HA for multiple non-bisync syncers
- Negative assertions that non-bisync runs do not create
  `redis-gunyu-bisync:*` metadata

Optional environment-dependent runners cover auth/TLS, version matrices, and
etcd-backed control planes.

Unified local and CI entry points are available from the repository root:

```bash
make test-static
make test-unit
make test-race
make test-integration
make test-e2e-smoke
```

The integration and E2E runners create isolated Redis instances on validated
port blocks and write reproducibility evidence under `.artifacts/tests/`.
