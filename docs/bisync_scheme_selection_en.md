# Cluster Bisync Marker Scheme Selection

- [Cluster Bisync Marker Scheme Selection](#cluster-bisync-marker-scheme-selection)
  - [1. Background](#1-background)
  - [2. Goals and Non-Goals](#2-goals-and-non-goals)
  - [3. Current Implementation Status](#3-current-implementation-status)
  - [4. Evaluation Criteria](#4-evaluation-criteria)
  - [5. Scheme 1: Slot-Scoped Marker with Real Transactions](#5-scheme-1-slot-scoped-marker-with-real-transactions)
  - [6. Scheme 2: Redis-Node-Scoped Marker with Legacy Pseudo-Batching](#6-scheme-2-redis-node-scoped-marker-with-legacy-pseudo-batching)
  - [7. One Marker for One Command or Multiple Commands](#7-one-marker-for-one-command-or-multiple-commands)
  - [8. Scheme Comparison](#8-scheme-comparison)
  - [9. Recommended Scheme](#9-recommended-scheme)
  - [10. Future Direction](#10-future-direction)
  - [11. Final Conclusion](#11-final-conclusion)

## 1. Background

The real-time synchronization pipeline in `redis-GunYu` is:

- input: use `PSYNC` to read from source Redis nodes
- channel: persist local cache
- output: replay parsed RDB/AOF commands to the target Redis

The recovery axis is still one linear replication stream per source Redis node. The recovery point is described by source `runId + offset`.

The core problem in bidirectional synchronization is not "how to write the same command again". The real problems are:

- how to prevent A -> B mirrored writes from flowing back through B -> A
- how to keep recovery semantics valid in Redis cluster mode

This document focuses on the marker design for cluster output, evaluates candidate schemes, and explains the selected engineering scheme.

Notes:

- This is a cluster-oriented scheme-selection document.
- The current code also supports the real-transaction bisync path for standalone Redis.
- The codebase still keeps the `parallel` branch for scheme comparison and internal validation, but current benchmark results do not show a stable throughput or tail-latency advantage, so it is not recommended as a default mode.
- For implementation details, see [bisync_scheme1_impl_en.md](./bisync_scheme1_impl_en.md).

## 2. Goals and Non-Goals

Goals:

- design a practical marker scheme for cluster bidirectional sync
- define boundaries, failure modes, and recovery costs for each scheme
- select the engineering scheme used by the implementation

Non-goals:

- solving business-level conflicts such as LWW, CRDT, or vector clocks
- turning redis-GunYu into a strongly consistent multi-master database
- documenting every standalone implementation detail

## 3. Current Implementation Status

The recovery semantics are still source-stream based:

- each source Redis node provides a linear replication stream
- checkpoint data describes how far that source stream has been safely replayed
- bisync stores recovery metadata under a stable `checkpointName` namespace
- `runId/replid` is used to look up `redis-gunyu-checkpoint-hash`; it is not itself the bisync namespace

The current bisync main path is no longer the old cluster pseudo-batch path. It submits each replay unit to the target Redis as a real `MULTI/EXEC` transaction.

Current characteristics:

- transaction writes `marker + business commands + latest/commit(+index)`
- when `mode=sync`, slot-local `latest` records drive recovery
- when `mode=pipeline` or `parallel`, `commit record + commit index + checkpointName:frontier` drive recovery
- bisync namespace is derived from a stable `checkpointName`
- recovery in cluster mode scans all `16384` slots by default, so target resharding or failover does not hide old metadata
- target topology changes are handled by retrying the whole replay unit on `MOVED` / `ASK`

## 4. Evaluation Criteria

Every scheme is evaluated by:

1. Loop suppression correctness

   Can the marker reliably identify mirrored traffic?

2. Recovery semantics

   Can restart recover from a clear and monotonic checkpoint?

3. Implementation complexity

   How much of the existing architecture must change?

4. Performance cost

   Does the scheme significantly increase writes or reduce throughput?

5. Safety for non-idempotent commands

   Examples include `INCR`, `LPUSH`, and `XADD`.

6. Cluster compatibility

   Does the scheme work with slots, nodes, failover, and topology changes?

## 5. Scheme 1: Slot-Scoped Marker with Real Transactions

Core idea:

- the marker follows a replay unit rather than a Redis node
- each replay unit must map to a clear slot-local transaction unit
- the target Redis sees a real transaction:

```redis
MULTI
SET <slot-marker-key> <marker-value> PX <ttl>
... business commands ...
HSET/ZADD ... checkpoint or journal ...
EXEC
```

Marker meaning:

- business commands in this transaction came from mirrored replay
- the reverse link should suppress the whole transaction

Advantages:

- The marker boundary is exactly the Redis transaction boundary.
- The reverse parser only needs to inspect `MULTI ... EXEC`.
- Multi-command source transactions can be preserved when they are single-slot in cluster mode.
- Business commands and recovery metadata can share the same commit boundary.

Main risk: recovery is harder than sending.

The source stream is node-linear, but target commits become slot-local transactions. If several units are in flight concurrently, a later offset may commit before an earlier offset. A simple "max offset" checkpoint may skip uncommitted work; a simple "min offset" checkpoint may replay committed non-idempotent commands.

There are two common checkpoint approaches:

1. Write checkpoint data inside the same `MULTI/EXEC`.

   This is correct in fully `sync` mode because committed units form a contiguous prefix. It is not enough in concurrent mode because slot-local progress does not imply a global contiguous prefix.

2. Write checkpoint data periodically outside the transaction.

   This is simpler, but business writes and checkpoint updates do not share a crash boundary. It can be used as a snapshot/cache but not as the only authoritative checkpoint for bisync.

Therefore concurrent scheme 1 needs an explicit global contiguous frontier mechanism:

- each committed unit can be recorded individually
- a global frontier advances only when all earlier units are known to be committed

Cross-slot replay units are not valid for this scheme. If a unit touches multiple slots, it cannot be submitted as one real cluster transaction. Splitting such a unit worsens the mismatch between node-level source offsets and slot-level target commits, so the current strict path fails fast instead.

Conclusion:

- Scheme 1 has the highest implementation cost.
- It is the only scheme whose marker boundary is visible and enforceable by Redis itself.
- It is the scheme selected by the current implementation.

## 6. Scheme 2: Redis-Node-Scoped Marker with Legacy Pseudo-Batching

Core idea:

- keep the current source-node recovery axis
- keep the legacy cluster pseudo-batch output path
- insert marker commands as hints in the output stream

This family has two variants:

- 2A: one marker covers multiple following commands
- 2B: one marker covers only one following command

Why it is node-scoped:

- the authoritative checkpoint remains one source pipeline checkpoint
- restart still uses source-node `runId + offset`
- no per-slot checkpoint lane is introduced

Advantages:

- recovery shape is closest to the existing system
- implementation cost is low
- throughput is easier to preserve

Core problems:

- there is no real Redis transaction boundary
- marker and business commands are only adjacent by client-side convention
- partial success is possible
- correctness cannot rely on "commands usually arrive together"

### Scheme 2A: One Marker Followed by Multiple Commands

This reduces marker count and write amplification, but it has the worst correctness properties:

- marker coverage is only an output-side convention
- the reverse parser cannot reliably know how many commands belong to the marker
- partial success makes recovery ambiguous
- non-idempotent commands may be replayed and amplified

Conclusion: not recommended.

### Scheme 2B: One Marker Followed by One Command

This reduces the association scope to the minimum:

```redis
SET <shard-syncer-marker-key> <marker-value> PX <ttl>
<one business command>
```

A stronger 2B design would require:

- deterministic marker keys per target shard and per syncer pipeline
- marker and business command routed to the same target shard
- command digest in the marker
- parser state keyed by `syncerID`, not one global pending marker
- best-effort behavior for malformed or unmatched markers

Even with these improvements, 2B still lacks a Redis-visible transaction boundary. The marker and business command can be separated by routing, retry, connection switching, reply ordering, or interleaving with real client writes. If the marker cannot match its business command, mirrored writes re-enter the business path and may loop.

For non-idempotent commands, this is structurally unsafe.

Conclusion: 2B is not a valid fallback for scheme 1.

## 7. One Marker for One Command or Multiple Commands

This is not an independent scheme; it is the key split inside scheme 2.

One marker for multiple commands is only acceptable when a real transaction boundary exists. Under pseudo-batching it is unsafe.

One marker for one command is the best possible shape when no transaction boundary exists, but it is still not enough for cluster correctness because the marker-command relationship remains a weak adjacency assumption.

## 8. Scheme Comparison

| Dimension | Scheme 1: slot-scoped real transaction | Scheme 2A: node-scoped marker for many commands | Scheme 2B: node-scoped marker for one command |
| --- | --- | --- | --- |
| Loop suppression correctness | High | Low | Medium in best case, not provable |
| Recovery clarity | Medium; needs frontier/journal | Low | Low |
| Implementation complexity | Highest | Low | Medium |
| Performance | Medium | High | Low to medium |
| Non-idempotent command safety | High | Worst | Low |
| Fit with existing architecture | Medium | Medium | Superficially high, but correctness fails |
| Recommendation | Selected | Not recommended | Not accepted |

## 9. Recommended Scheme

The selected scheme is scheme 1:

- marker is slot-scoped
- marker, business commands, and checkpoint/journal metadata are written in a real `MULTI/EXEC`
- reverse suppression is based on the transaction boundary
- recovery is modeled by `latest` or `frontier + commit record`, not by weak pending-marker matching

Reasons:

1. The transaction boundary must be visible to Redis.

   The system needs to know which writes are mirrored and what exact unit should be suppressed by the reverse link. This only becomes robust when marker, business commands, and metadata are bound by `EXEC`.

2. Scheme 2B cannot be proven correct in cluster mode.

   It assumes stable adjacency between marker and business command. That assumption breaks under pseudo-batching, retries, connection changes, partial success, and interleaving.

3. Recovery complexity is unavoidable.

   Cutting a source stream into slot-local replay units requires a real recovery model. Frontier and journal are not optional polish; they are part of the correctness model.

Rejected schemes:

- 2A is strongly rejected because one marker for many pseudo-batch commands has no reliable boundary.
- 2B is explicitly not used as a fallback because unmatched markers can let mirrored writes re-enter the business path.

## 10. Future Direction

Future work should continue strengthening scheme 1:

- namespace-local slot hints or active slot indexes may be added as caches, but recovery must still fall back to all-slot scanning when needed
- more fault-injection coverage for target resharding, failover, and source runId changes
- keep `bisyncEnabled` as the only explicit bisync switch
- continue moving cluster-global opcodes and special RDB paths into verifiable bisync control flow

Ideas that should not be kept:

- 2A: one marker covering multiple pseudo-transaction commands
- 2B: one marker for one command with pending-marker weak matching
- any correctness assumption based on commands "usually" being contiguous or not interleaved

## 11. Final Conclusion

For cluster bisync in `redis-GunYu`:

- the transaction boundary must be visible to Redis
- recovery complexity cannot be bypassed by weak marker association

Therefore the chosen scheme is:

- slot-scoped marker with real transactions
- reverse suppression by transaction boundary
- recovery by durable `latest` or `frontier + commit record`

Scheme 2A and 2B are rejected and should not be used as fallback designs.
