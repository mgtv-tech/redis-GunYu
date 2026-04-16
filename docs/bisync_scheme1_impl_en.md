# Bisync Scheme 1 Design and Implementation

- [Bisync Scheme 1 Design and Implementation](#bisync-scheme-1-design-and-implementation)
  - [1. Purpose](#1-purpose)
  - [2. Architecture](#2-architecture)
  - [3. Design](#3-design)
  - [4. Observability](#4-observability)
  - [5. Implementation and Tests](#5-implementation-and-tests)
  - [6. Current Limitations](#6-current-limitations)
  - [7. Future Work](#7-future-work)

## 1. Purpose

This document describes the implemented "scheme 1" bisync design in `redis-GunYu`.

It solves three core problems:

- mirrored writes from A -> B must not be replayed back by B -> A
- cluster output must use a real and provable transaction boundary rather than pseudo-batching
- restart must recover an authoritative source `run_id + offset` from target-side control metadata

The current cluster bisync implementation accepts only the scheme 1 main path:

- no 2A/2B pending-marker weak matching fallback
- no correctness assumption based on marker and business commands "usually" arriving contiguously
- if a replay unit cannot prove strict routing, single-slot ownership, and real transaction commit, it fails fast or follows a documented legacy special path

Scheme 1 is enabled explicitly by:

```yaml
output:
  replay:
    bisyncEnabled: true
```

Important distinction:

- whether bisync is enabled is decided only by `bisyncEnabled`
- whether the normal non-bisync replay path prefers transactional batching is controlled by `replayTransaction` and runtime capability

`CanTransaction` no longer decides the bisync switch.

Main implementation files:

- [syncer/output.go](../syncer/output.go)
- [syncer/bisync.go](../syncer/bisync.go)
- [syncer/bisync_rdb.go](../syncer/bisync_rdb.go)
- [pkg/redis/checkpoint/bisync.go](../pkg/redis/checkpoint/bisync.go)
- [pkg/redis/client/cluster/cluster.go](../pkg/redis/client/cluster/cluster.go)
- [pkg/redis/client/cluster/txn_batcher.go](../pkg/redis/client/cluster/txn_batcher.go)
- [pkg/redis/keyspec/keyspec.go](../pkg/redis/keyspec/keyspec.go)

## 2. Architecture

Scheme 1 turns source replication input into replay units, then submits each replay unit to the target Redis as a real `MULTI/EXEC`.

```mermaid
flowchart LR
  A["source PSYNC stream"] --> B["AOF parser / RDB workers"]
  B --> C["Replay Unit builder"]
  C --> D["key routing + slot validation"]
  D --> E["real MULTI/EXEC sender"]
  E --> F["bisync metadata"]
  F --> G["sync latest or pipeline/parallel frontier"]
  E --> H["reverse parser suppression"]
```

Responsibilities:

1. Parser layer: convert AOF/RDB input to replay units.
2. Constraint layer: parse business keys, validate strict routing, single-slot ownership, and filter projection safety.
3. Commit layer: write marker, business commands, and checkpoint/journal in one real transaction.
4. Recovery layer: recover start point from `latest` or `frontier + commit index + commit record`.
5. Suppression layer: reverse parser identifies mirrored transactions and drops the whole unit.

## 3. Design

### 3.1 Core Principles

Scheme 1 follows these principles:

1. Commit boundary must match Redis's own transaction boundary.
2. The bisync namespace must be independent of target slot view, source address, and replid; it uses stable `checkpointName`.
3. Authoritative recovery points must come from deterministic keys, not random scanning.
4. Cluster correctness depends on real key extraction and single-slot validation, not `args[0]`.
5. Full-sync loop suppression and authoritative recovery boundaries are separate concepts.

Control data has two roles:

- suppression plane: marker used to identify mirrored transactions
- recovery plane: `latest`, `frontier`, `commit index`, and `commit record`

They are often written in the same transaction but serve different purposes.

### 3.2 Replay Unit Model

`bisyncReplayUnit` is defined in [syncer/bisync.go](../syncer/bisync.go).

Core fields:

- `Seq`: globally increasing sequence on the source stream
- `StartOffset` / `EndOffset`: source replication stream boundary covered by the unit
- `Slot` / `SlotTag`: slot-local control dimension
- `Digest`: stable digest of business commands
- `SourceTxn`: whether the unit came from source `MULTI ... EXEC`
- `Commands`: business commands to replay

Cutting rules:

- normal command: one command is one unit
- source `MULTI ... EXEC`: the whole transaction is one unit
- `PING`, `SELECT`, and checkpoint namespace commands do not enter replay units
- recognized mirrored transactions are suppressed as a whole

Cluster vs standalone:

- cluster: all business keys in one unit must be provably in the same slot
- standalone: use synthetic slot `0`, allowing arbitrary key distribution

RDB path:

- single-key entry: one key becomes one unit
- split key: each bin becomes a unit, while bins of the same key stay on the same worker
- `StartOffset == EndOffset == fullSyncOffset`

The RDB `fullSyncOffset` is a full-sync barrier label. It is not a per-key authoritative recovery point.

### 3.3 Control-Plane Key Layout

The current implementation uses stable `checkpointName` as the bisync namespace identity.

Startup flow:

1. Use the current source `runId/replid2` to look up `redis-gunyu-checkpoint-hash`.
2. If found, reuse the existing `checkpointName`.
3. If not found, create a new stable root such as `redis-gunyu-checkpoint-bisync:<stable-id>`.
4. Write `runId -> checkpointName` back to `checkpoint-hash`.

Key layout:

| Key | Scope | Purpose | Authoritative |
| --- | --- | --- | --- |
| `checkpointName` | global | full-sync barrier or legacy checkpoint root | yes |
| `checkpointName:frontier` | namespace-global | global contiguous prefix snapshot in `pipeline`/`parallel` mode | yes |
| `redis-gunyu-bisync:<checkpointName>:marker:{slotTag}` | slot-local | mirrored transaction suppression | no |
| `redis-gunyu-bisync:<checkpointName>:latest:{slotTag}` | slot-local | latest committed point in `sync` mode | yes |
| `redis-gunyu-bisync:<checkpointName>:index:{slotTag}` | slot-local | commit record index in `pipeline`/`parallel` mode | no, index only |
| `redis-gunyu-bisync:<checkpointName>:commit:{slotTag}:<unitSeq>` | slot-local | commit journal record in `pipeline`/`parallel` mode | yes, with frontier |

`slotTag = BisyncSlotTag(slot)` is chosen so that the hash tag maps to the intended Redis cluster slot. This keeps slot-local control keys colocated with the business key and eligible for the same `MULTI/EXEC`.

Standalone mode uses:

- `slot = 0`
- `slotTag = BisyncSlotTag(0)`

### 3.4 Routing and Slot Constraints

Cluster correctness depends on proving the real command key set.

Key resolution order:

1. project static keyspec from `pkg/redis/keyspec`
2. fall back to target Redis `COMMAND GETKEYS` when needed
3. fail fast if key extraction cannot be proven

Cluster mode rejects:

- unresolved command keys
- cross-slot replay units
- unsafe filtered transaction projection

Standalone mode uses a synthetic slot and does not require real Redis cluster slot colocation.

### 3.5 Filters and Transaction Projection

Filters may remove commands or keys from the source stream.

For a transaction:

- if all commands are filtered out, no unit is emitted
- if part of the transaction remains, the remaining commands must still be safe to replay
- only limited multi-key partial projection is allowed, such as `MSET`, `DEL`, and `UNLINK`

Unsafe projection fails rather than silently changing semantics.

### 3.6 Transaction Control Data

Each bisync transaction begins with a marker:

```redis
SET redis-gunyu-bisync:<checkpointName>:marker:{slotTag} <marker-value> PX <ttl>
```

The marker contains enough information for the reverse parser to identify a GunYu mirrored transaction.

`sync` writes a `latest` record in the same transaction:

```redis
HSET redis-gunyu-bisync:<checkpointName>:latest:{slotTag} ...
```

`pipeline`/`parallel` write:

```redis
HSET redis-gunyu-bisync:<checkpointName>:commit:{slotTag}:<unitSeq> ...
ZADD redis-gunyu-bisync:<checkpointName>:index:{slotTag} <unitSeq> <commit-key>
```

The frontier snapshot is namespace-global:

```redis
HSET <checkpointName>:frontier ...
```

It is advanced only when committed records form a contiguous prefix.

RDB replay also writes a marker and business commands in a transaction, but does not treat each key as an authoritative AOF recovery point.

### 3.7 Sending Path

AOF sending has two stages:

1. parse AOF and emit replay units
2. commit replay units to the target Redis

When `mode=sync`:

- units are committed serially
- each successful transaction writes the slot-local `latest`
- `latest` is the recovery source for startup

When `mode=pipeline` or `parallel`:

- `pipeline` keeps one dispatch connection and receives replies in send order
- `parallel` commits units through bounded per-slot lanes
- each commit writes a commit record and index
- `bisyncFrontierCoordinator` advances the global frontier only for a contiguous committed prefix
- commit records already covered by frontier are garbage-collected

RDB sending:

- uses the existing parallel RDB replay worker model
- each worker builds bisync RDB replay units
- key-scoped RDB entries are replayed with marker + business commands
- cluster-global RDB entries require special handling because they do not map naturally to one slot-local key

Target topology changes:

- cluster transaction batcher handles `MOVED` / `ASK`
- the whole replay unit is retried, not a partial command tail
- persistent instability still fails stop

### 3.8 Recovery Path

Startup recovery entry:

1. resolve stable bisync `checkpointName`
2. determine bisync mode: `sync`, `pipeline`, or `parallel`
3. load the appropriate recovery metadata
4. return source start point and restore local bisync sequence/offset

`sync`:

- scan all recovery slots for `latest:{slotTag}`
- choose the best latest record matching current runIDs
- recover from its source offset and sequence

`pipeline`/`parallel`:

- load `<checkpointName>:frontier`
- load commit records after `frontier.UnitSeq`
- rebuild the contiguous frontier
- recover from the rebuilt frontier offset

RDB recovery boundary:

- RDB replay writes markers for suppression
- authoritative recovery still depends on the full-sync checkpoint barrier
- per-key RDB replay units are not treated as standalone authoritative checkpoints

### 3.9 Frontier and GC

`pipeline`/`parallel` need a global contiguous frontier because commits may complete out of order.

The coordinator keeps:

- current frontier snapshot
- pending commit records
- commit backlog metrics
- GC list for commit records already covered by frontier

When a new commit record arrives:

1. add it to pending
2. advance frontier while the next sequence exists
3. save the new frontier snapshot
4. delete covered commit records and remove their index entries

This turns out-of-order per-slot commits into a source-stream contiguous recovery point.

## 4. Observability

Bisync metrics include:

- `bisync_unit_build`: replay unit build count
- `bisync_txn_commit`: transaction commit count
- `bisync_single_slot_fail`: single-slot validation failures
- `bisync_txn_suppress`: mirrored transaction suppressions
- `bisync_frontier_seq`: `pipeline`/`parallel` frontier sequence
- `bisync_frontier_offset`: `pipeline`/`parallel` frontier offset
- `bisync_frontier_rebuild_seconds`: startup frontier rebuild duration
- `bisync_commit_backlog`: pending commit backlog
- `bisync_commit_gc`: commit record GC count

These metrics should be watched together with syncer status, Redis memory, goroutines, and storer directory growth.

## 5. Implementation and Tests

Implementation split:

- [syncer/bisync.go](../syncer/bisync.go): AOF replay units, marker suppression, `sync`/`pipeline`/`parallel` senders, frontier coordinator
- [syncer/bisync_rdb.go](../syncer/bisync_rdb.go): RDB replay units, RDB `keyExists` semantics, RDB bisync transactions
- [pkg/redis/checkpoint/bisync.go](../pkg/redis/checkpoint/bisync.go): key encoding, record/frontier encoding, frontier rebuild, latest/journal loading
- [pkg/redis/client/cluster/txn_batcher.go](../pkg/redis/client/cluster/txn_batcher.go): real cluster transaction batcher and redirect retry
- [pkg/redis/keyspec/keyspec.go](../pkg/redis/keyspec/keyspec.go): static command keyspec
- [pkg/filter/filter.go](../pkg/filter/filter.go): filters and partial projection

Focused unit tests cover:

- replay unit building
- cluster cross-slot failure
- standalone synthetic slot behavior
- AOF mirrored transaction suppression
- RDB mirrored transaction suppression
- RDB `replace/ignore/error` behavior
- split-key `skipKey`
- strict routing and `COMMAND GETKEYS` fallback
- filtered transaction projection
- frontier rebuild
- checkpointName as bisync namespace root
- all-slot cluster recovery scanning
- bisync remains enabled when `CanTransaction=false`
- cluster transaction batcher retry on `MOVED` / `ASK`

Integration tests in `tests/bisync`:

- [tests/bisync/run_category1.sh](../tests/bisync/run_category1.sh): basic bidirectional convergence
- [tests/bisync/run_category2.sh](../tests/bisync/run_category2.sh): `sync` / `pipeline` / `parallel` restart and resume
- [tests/bisync/run_category3.sh](../tests/bisync/run_category3.sh): RDB special paths and full-sync barrier
- [tests/bisync/run_category4.sh](../tests/bisync/run_category4.sh): filters, keyspec, strict routing, `COMMAND GETKEYS`
- [tests/bisync/run_category5.sh](../tests/bisync/run_category5.sh): failover and topology disturbance

See [tests/bisync/README.md](../tests/bisync/README.md) for the full test runner set.

## 6. Current Limitations

1. Scheme 1 correctness depends on provable key extraction.

   If static keyspec and `COMMAND GETKEYS` cannot prove the real key set, the strict path fails stop.

2. Partial projection is intentionally limited.

   Only selected commands such as `MSET`, `DEL`, and `UNLINK` support safe projection.

3. RDB replay currently solves mirrored transaction suppression, not per-key authoritative recovery.

   Recovery still uses the full-RDB checkpoint barrier.

4. Cluster non-key-based global opcodes still have legacy boundaries.

   Commands such as `FUNCTION RESTORE` cannot be fully represented as slot-local bisync transactions.

5. `pipeline`/`parallel` recovery can detect journal gaps but does not yet skip every individually committed unit after a gap.

   It recovers from the last contiguous committed prefix.

6. Severe cluster topology instability still fails stop.

   `MOVED` / `ASK` transaction-level retry is supported, but persistent instability is not converted into best-effort replay.

7. Module command compatibility is not closed.

   RedisJSON / RedisBloom commands such as `JSON.SET`, `JSON.DEL`, `JSON.MSET`, `BF.ADD`, `CMS.MERGE`, `TDIGEST.MERGE`, and `TOPK.ADD` are still managed as temporarily unsupported until module-instance verification, keyspec validation, strict routing, and release gates are complete.

## 7. Future Work

Priorities:

1. Continue improving and validating keyspec coverage.

   `run_category4.sh` and `keyspec_verify` already support custom Redis binaries, `loadmodule` arguments, external addresses, and additional sample files.

2. Close module command support.

   Add RedisJSON / RedisBloom instances to regular validation before removing the unsupported status of module commands.

3. Strengthen `pipeline`/`parallel` recovery checks.

   Detect snapshot rollback, journal gaps, and cross-runID pollution more explicitly.

4. Precisely skip already committed units after frontier.

   Startup should load durable commit records after the frontier into a recovery-time committed view. If the parser later regenerates a unit whose `unit_seq + run_id + offset range + slot + digest` matches a durable commit record, it can skip sending it.

5. Optimize replay unit granularity and parallelism.

   Same-slot commands that are provably safe could be merged into larger units to reduce transaction and control-plane overhead.

6. Add observability for keyspec fallback.

   Track which commands use `COMMAND GETKEYS` and which remain unresolved.

7. Continue aligning RDB and AOF control-plane models.

   Stronger full-sync recovery requires a clearer model between parallel RDB workers and global barriers.

8. Evaluate cluster-global RDB opcode handling.

   The goal is to make global-object replay suppressible and verifiable under the bisync control plane.
