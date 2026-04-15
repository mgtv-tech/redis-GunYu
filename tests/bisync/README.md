# Bisync Tests

`tests/bisync` contains executable end-to-end checks for bidirectional sync.

Release-oriented test scope and gating criteria are documented in
[tests/bisync/test_design.md](../../tests/bisync/test_design.md).

Current scripts:

- `run_category1.sh`
  Starts two isolated local Redis clusters, launches GunYu in both directions, writes a mixed set of idempotent and non-idempotent commands, and verifies both clusters converge to identical business data by comparing logical key state for a test prefix.

- `run_category2.sh`
  Runs restart and resume checks in both serial mode and pipeline mode. Each scenario writes data, waits for convergence, stops both syncers, writes more data while replication is offline, restarts both syncers, then verifies exact final business values, cluster equality, non-zero PSYNC resume offsets, and the expected bisync metadata shape (`latest` for serial, `frontier` for pipeline).

- `run_category3.sh`
  Runs the RDB-special-path test set. It first executes focused `syncer` unit tests for mirrored RDB transaction suppression, `keyExists=replace/ignore/error`, RESTORE-vs-expanded replay selection, split-key skip handling, and marker-based replay. It then runs a pure full-sync integration check in serial mode and pipeline mode to verify that RDB writes converge on business data while the target only exposes transient bisync markers, with no authoritative `latest`/`commit`/`frontier` metadata before any AOF replay. The current main path does not persist standalone `:rdb:` records.

- `run_category4.sh`
  Runs the filter and command-routing test set. It executes focused unit tests for key extraction, unsafe projection rejection, slot-only filters, strict cluster routing, `COMMAND GETKEYS` fallback, and same-slot transaction batching. It then starts a local Redis cluster and runs `keyspec_verify` against a real node to compare the project's static `keyspec.CommandKeys` results with Redis `COMMAND GETKEYS`.
  The runner can also be pointed at newer Redis builds or module-enabled instances:
  `KEYSPEC_REDIS_SERVER=/path/to/redis-server KEYSPEC_REDIS_SERVER_ARGS="--loadmodule /path/to/rejson.so --loadmodule /path/to/redisbloom.so" bash ./tests/bisync/run_category4.sh`
  Or resolve the binary from a deployed Redis root:
  `KEYSPEC_REDIS_DEPLOY_ROOT=/path/to/redis-deploy bash ./tests/bisync/run_category4.sh`
  Or skip local startup and verify external addresses directly:
  `KEYSPEC_VERIFY_ADDRS="127.0.0.1:7000,127.0.0.1:7100" KEYSPEC_VERIFY_TAGS="core,module" bash ./tests/bisync/run_category4.sh`
  Or infer external addresses from a deployed cluster directory containing `redis.conf` files:
  `KEYSPEC_VERIFY_DEPLOY_ROOT=/path/to/c1 bash ./tests/bisync/run_category4.sh`
  `keyspec_verify` also accepts `--samples-file extra.json` so newer-version or module-only command cases can be appended without changing Go code. JSON entries use `{ "name": "...", "cmd": "...", "args": ["..."], "tags": ["..."] }`.

- `run_category5.sh`
  Runs the fault-injection and topology-disturbance test set. It executes the existing topology unit suite, then starts source and target Redis clusters with replicas in serial mode and pipeline mode, launches GunYu in both directions, injects source failover, target failover, and syncer restart, writes on both clusters after each disturbance, and verifies both clusters converge to identical exact business state.

- `run_category6.sh`
  Runs the external-cluster mixed-structure integration test set against already started clusters such as `c1/c2`. It flushes both clusters, starts bisync in both directions, writes strings, hashes, lists, sets, zsets, and large variants of those structures, waits for convergence, and emits a Markdown report with workload stats, status snapshots, and compare results.

- `run_category7.sh`
  Runs the external-cluster soak integration test set against already started clusters such as `c1/c2`. It flushes both clusters, starts bisync in both directions, performs sustained bidirectional writes for a configurable duration, waits for convergence, and emits a Markdown report with workload stats, status snapshots, and compare results.

- `run_benchmark.sh`
  Runs the external-cluster benchmark path against already started clusters such as `c1/c2`. It sweeps target QPS values for serial and/or pipeline mode, starts bisync in both directions, runs sustained bidirectional writes, samples sync delay from the existing `input.syncDelayTestKey` Prometheus metric, samples syncer CPU/RSS/goroutines/storer size, verifies final stable-key convergence, and emits one Markdown report per mode/QPS pair.

- `run_category8.sh`
  Runs a non-bisync regression check against temporary local Redis clusters. It starts a single-direction cluster sync with `bisyncEnabled: false`, verifies normal business convergence, and asserts that no bisync metadata namespace is created on the target.

- `run_category9.sh`
  Runs the release durability soak test against temporary local Redis clusters with replicas and AOF enabled. It supports manually gated tiers with `SOAK_TIER=2h`, `SOAK_TIER=4h`, and `SOAK_TIER=6h`. Each invocation runs only the selected tier, injects scheduled syncer restarts, Redis failovers, and an offline-syncer resume window while the workload continues writing, then emits per-mode Markdown reports plus JSONL resource samples. Review and accept the report before starting the next tier.

Binary and deploy-root discovery:

- `run_category1/2/3/5/8.sh` accept either `REDIS_SERVER_BIN=/path/to/redis-server` or `REDIS_DEPLOY_ROOT=/path/to/redis-deploy`.
- `run_category4.sh` accepts either `KEYSPEC_REDIS_SERVER=/path/to/redis-server` or `KEYSPEC_REDIS_DEPLOY_ROOT=/path/to/redis-deploy`.
- `run_category6/7.sh` accept `LEFT_ADDRS` / `RIGHT_ADDRS` directly, or infer them from `LEFT_REDIS_DEPLOY_ROOT` / `RIGHT_REDIS_DEPLOY_ROOT` by scanning `redis.conf` files under those directories.

Durability soak examples:

```bash
SOAK_TIER=2h SCENARIOS=serial KEEP_TMP=1 bash ./tests/bisync/run_category9.sh
SOAK_TIER=4h SCENARIOS=serial KEEP_TMP=1 bash ./tests/bisync/run_category9.sh
SOAK_TIER=6h SCENARIOS=serial KEEP_TMP=1 bash ./tests/bisync/run_category9.sh
```

Run `SCENARIOS=pipeline` for the pipeline path, or `SCENARIOS=serial,pipeline` to run both paths sequentially for the selected tier. Reports are written under `${TMPDIR:-/tmp}/redisgunyu-bisync-cat9-${SOAK_TIER}` and are preserved for review.

The default category9 workload is capped by combined command throughput with `SOAK_TARGET_QPS=10000` and uses `SOAK_WORKERS=4` concurrent writer pairs. Set `SOAK_TARGET_QPS=0` to disable the limiter, or override it for a different durability target. If the report detects a goroutine growth warning, it writes forward and reverse goroutine dumps next to the report.

Benchmark examples:

```bash
BENCH_DURATION=15m BENCH_TARGET_QPS_LIST=1000,5000,10000 BENCH_WORKERS=4 SCENARIOS=serial,pipeline bash ./tests/bisync/run_benchmark.sh
SYNC_DELAY_MAX_MS=1000 BENCH_DURATION=30m BENCH_TARGET_QPS_LIST=10000 SCENARIOS=pipeline bash ./tests/bisync/run_benchmark.sh
```

`run_benchmark.sh` enables per-direction `input.syncDelayTestKey` probes by default. Reports include left-to-right and right-to-left sync-delay p50/p95/p99/max values sampled from each syncer's `/prometheus` endpoint. Set `SYNC_DELAY_MAX_MS=<milliseconds>` to turn the delay check into a hard gate; the default `0` records the numbers without failing the run. `SYNC_DELAY_SAMPLE_INTERVAL_SECONDS` controls the sampling cadence, and `SYNC_DELAY_TEST_KEY_PREFIX` controls the probe key prefix.

`run_category9.sh` validates the local `redis-server` major version before starting. Use Redis 7 for the normal durability gate by setting `REDIS_SERVER_BIN=/path/to/redis-server` when needed. Redis 8 currently exercises an unsupported RDB format path in this codebase; set `ALLOW_UNSUPPORTED_REDIS=1` only when the goal is compatibility investigation rather than release gating.
