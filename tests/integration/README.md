# Integration and regression runners

This directory contains the repository-level test orchestration used by local
development and GitHub Actions. All generated evidence is written below
`.artifacts/tests/` by default.

## Local entry points

| Command | Purpose | Typical duration |
| --- | --- | --- |
| `make test-static` | Formatting, vet, build, and shell syntax | minutes |
| `make test-coverage` | Unit tests and coverage threshold | minutes |
| `make test-race` | Core race-detector packages | minutes |
| `make test-race-all` | Full repository race detector | tens of minutes |
| `make test-integration` | Required Go integration tests against owned standalone and cluster Redis | minutes |
| `make test-e2e-smoke` | PR-sized non-bisync and bisync data-path smoke | minutes |
| `make test-nightly` | Core non-bisync and bisync category suites | longer-running |
| `make test-etcd ETCD_BIN=/path/to/etcd` | Optional etcd control-plane suite | minutes |
| `make test-upgrade-rollback PREVIOUS_GUNYU_BIN=/path/to/redisGunYu` | Checkpoint compatibility across previous/current/previous processes | minutes |
| `make test-sentinel-ha` | Two-GunYu Sentinel election and handover with continuous writes | minutes |
| `make test-sentinel-security` | Separate Sentinel/data ACL users and TLS permutation matrix | minutes |
| `make test-sentinel-upgrade-rollback PREVIOUS_GUNYU_BIN=/path/to/redisGunYu` | Previous direct mode to current Sentinel mode and rollback | minutes |

`redis-server` and `redis-cli` must be available in `PATH`. To select another
server binary, set `REDIS_SERVER_BIN=/absolute/path/to/redis-server` and put its
matching `redis-cli` directory first in `PATH`.

## Platform support

The Bash runners support Linux and macOS on `amd64` and `arm64`. They use
portable command forms shared by BSD and GNU userlands. `ripgrep` is optional;
when it is absent, regex checks fall back to `grep -E`. Missing dependencies
are reported with platform-neutral installation guidance rather than a command
for one package manager.

Temporary files use `TMPDIR` when it is set and otherwise use `/tmp`. Redis
Modules artifacts stay inside the per-run temporary directory. The etcd
installer selects the official Linux `tar.gz` or macOS `zip` asset based on
`uname`, including native Apple Silicon packages. Native Windows shells are not
supported; use Linux, macOS, a Linux container, or WSL.

`make test-static` also forces the regex helpers through their `grep` fallback
and verifies that dependency diagnostics remain package-manager neutral. The
pull-request workflow runs this gate on both Ubuntu and macOS.

## Runners

- `run_go_integration.sh` starts one standalone and one 3-master/3-replica
  cluster, exports the discovered topology, and runs required integration
  tests with `-count=1`. The Sentinel client integration test additionally
  creates and cleans up its own 3-Sentinel/1-master/2-replica topology. Missing
  or skipped tests are rejected using `testjson_gate`.
- `run_e2e_smoke.sh` runs representative standalone resume, cluster pipeline,
  Sentinel source/target failover, two-GunYu Sentinel handover, bidirectional
  sync, and bidirectional pipeline resume cases on dynamic ports.
- `run_nightly.sh` dispatches the core, resilience, security, module, and
  external-cluster suites selected with `NIGHTLY_SUITE`. The etcd suite is not
  part of any default aggregate; it requires `NIGHTLY_SUITE=etcd` together with
  `ENABLE_ETCD_TESTS=1`.
- `run_external_cluster_regression.sh` creates two disposable clusters before
  invoking runners that flush data. Direct use of those underlying runners
  requires `ALLOW_DESTRUCTIVE_REDIS_TESTS=1` and a non-empty
  `TEST_ENVIRONMENT_ID`; non-loopback targets additionally require
  `ALLOW_NON_LOOPBACK_REDIS_TESTS=1`.
- `run_upgrade_rollback.sh` verifies that the same checkpoint/store can resume
  on previous, current, and previous binaries in sequence.
- `run_sentinel_security_matrix.sh` verifies distinct Sentinel/data ACL users,
  Sentinel-only TLS, data-only TLS, and combined TLS with real failovers.
- `run_sentinel_upgrade_rollback.sh` verifies the supported rollback boundary:
  previous releases use direct master addresses, while the current release uses
  Sentinel discovery; rollback resolves the current masters before restart.

Use `TEST_RUN_ID` to assign a reproducible run identifier and `ARTIFACT_ROOT`
to relocate evidence. Every successful or failed run keeps logs, manifests,
topology details, structured gate output, and summaries under its run directory.
The runners only terminate Redis processes whose PID files were created below
that run directory.

## Pinned dependencies

`install_redis_source.sh <version> <install-dir>` builds an exact numeric Redis
tag on Linux or macOS. `install_etcd.sh <version> <install-dir>` installs an
exact etcd release on Linux or macOS for `amd64` or `arm64`.
Installing etcd does not enable its tests. Run `make test-etcd` explicitly, or
manually dispatch the Nightly workflow with `run_etcd_tests` enabled.
Redis Modules tests default to the multi-architecture digest for
`redis/redis-stack-server:7.4.0-v8`; override `MODULE_IMAGE` only when explicitly
qualifying another image.

## Compatibility policy

| Redis line | Automated coverage | Status |
| --- | --- | --- |
| 7.4.1 | PR integration/E2E, full Nightly core, release qualification | Qualified baseline |
| 8.0.0 | PR integration and Nightly compatibility smoke | Compatibility gate |
| 8.6.2 | Locally verified standalone/cluster integration and E2E smoke | Core smoke verified; durability not qualified |

Redis 8 must not be described as durability-qualified until its 2h/4h/6h
sequence and release benchmark have passed with retained reports.
