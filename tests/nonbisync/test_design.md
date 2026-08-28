# Non-Bisync Automated Integration Test Design

## 1. Goal

This directory provides release-oriented regression coverage for the original
single-direction sync path after bisync changes landed.

The suite is meant to answer:

- Does normal non-bisync sync still converge correctly?
- Do restart, resume, and cluster topology changes still work when
  `bisyncEnabled=false`?
- Does the non-bisync path stay isolated from bisync metadata and recovery
  logic?

## 2. Scope

The test suite focuses on integration behaviors that unit tests cannot prove by
themselves:

- End-to-end source to target business convergence
- One-way semantics: target-side writes are not mirrored back
- Checkpoint persistence across restart
- Cluster source failover and target failover handling
- Mixed topology replay for `cluster -> standalone` and `standalone -> cluster`
- Output filter correctness for DB, prefix, and slot filtering
- RDB `keyExists` strategy correctness for `replace`, `ignore`, and `error`
- Rich command/data-type replay boundaries including streams, TTL, binary data,
  long keys, and large payloads
- Multi-syncer control-plane leader takeover
- Pipeline mode compatibility when bisync is disabled
- Single-direction soak stability under topology disturbance and offline gaps
- Negative regression checks that bisync metadata is never created

## 3. Test Matrix

| Category | Topology | Modes | Core checks |
| --- | --- | --- | --- |
| category1 | cluster -> cluster | `sync`, `pipeline` | basic convergence, transaction replay, delete replay, one-way semantics, no bisync metadata |
| category2 | cluster -> cluster | `sync`, `pipeline` | stop syncer, offline source writes, restart, resume from checkpoint, no bisync metadata |
| category3 | standalone -> standalone | `sync`, `pipeline` | basic convergence, restart/resume, one-way semantics, no bisync metadata |
| category4 | cluster(replica) -> cluster(replica) | `sync`, `pipeline` | source failover, target failover, syncer restart, final convergence, no bisync metadata |
| category5 | cluster(replica) -> cluster(replica) | `sync`, `pipeline` | hotspot-key writes across source failover, target failover, offline syncer gap, restart/resume, no bisync metadata |
| category6 | mixed topology | `sync`, `pipeline` | `cluster -> standalone` plus source failover/resume, `standalone -> cluster` plus target failover/resume |
| category7 | standalone + cluster matrix | `sync`, `pipeline` | RDB `keyExists`, `targetDbMap`, `dbBlacklist`, prefix filters, slot filters |
| category8 | cluster -> cluster | `sync`, `pipeline` | rich workload: binary strings, long keys, large payloads, streams, TTL edges, scripts, same-slot transactions |
| category9 | cluster(replica) -> cluster(replica) | `sync`, `pipeline` | segmented soak, source/target failover, syncer restart, offline catch-up, resource sampling |
| category10 | cluster(replica) -> standalone (HA control plane) | `sync`, `pipeline` | leader/follower pipelines initially paused, explicit resume, leader termination, follower takeover, no duplicate replay |

## 4. Assertions

Every category should enforce both positive and negative signals.

Positive:

- Business keys on the target match the expected final state.
- For cluster categories, source and target clusters compare equal for the test
  prefix.
- Root checkpoint metadata exists after sync starts.
- Restart scenarios recover and continue applying new writes.
- Mixed-topology scenarios converge even when only one side is clustered.
- `keyExists=ignore` preserves pre-existing target keys and
  `keyExists=error` fails fast instead of silently overwriting them.
- `targetDbMap`, DB filters, prefix filters, and slot filters project only the
  expected key set.
- Rich workloads preserve binary payloads, stream entries, long keys, and TTL
  boundaries as intended.
- Hotspot keys that are updated repeatedly across topology and restart windows
  still converge exactly once to the target.
- HA control-plane scenarios continue after leader termination without creating
  duplicate leaders.
- HA leader and follower pipelines remain inactive while initially paused and
  retain their resumed state across leader takeover.

Negative:

- No key under `redis-gunyu-bisync:*` is created.
- No bisync frontier/commit/latest namespace key is created.
- Logs do not contain bisync recovery markers such as
  `bisync startpoint` or `bisync checkpoint namespace`.
- One-way scenarios never replay target-only writes back to the source.
- Topology and restart recovery do not drop or duplicate writes on repeatedly
  updated keys.
- Filtered DBs, prefixes, and slots never leak into the target.
- `keyExists=error` does not partially apply non-conflicting data after the
  first conflicting full-sync failure.

## 5. Why Separate From `tests/bisync`

`tests/bisync/run_category8.sh` already performs a narrow non-bisync smoke
check. This suite expands that idea into an independent regression track with:

- explicit `sync` and `pipeline` mode coverage
- standalone topology coverage
- mixed-topology coverage
- configuration and filter matrix coverage
- rich workload and TTL/stream boundary coverage
- restart and resume coverage
- failover and topology-disturbance coverage
- single-direction soak coverage
- control-plane HA coverage
- a dedicated report entry point

Keeping it under `tests/nonbisync` makes the intent explicit and avoids mixing
single-direction release gates with bidirectional test categories.

## 6. Future Extensions

Optional runners already cover:

- auth and conditional TLS transport checks
- redis-server version matrices
- etcd-backed control-plane takeover

Recommended follow-up work after the current suite:

- upgrade and rollback scenarios across checkpoint formats
- module-heavy workloads
- external-cluster long-haul soak
