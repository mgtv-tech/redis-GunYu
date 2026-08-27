# Checkpoint Mechanism and Redis Key Reference

- [Checkpoint Mechanism and Redis Key Reference](#checkpoint-mechanism-and-redis-key-reference)
  - [1. Purpose](#1-purpose)
  - [2. Overview](#2-overview)
  - [3. Non-bisync checkpoint keys](#3-non-bisync-checkpoint-keys)
    - [3.1 `redis-gunyu-checkpoint-hash`](#31-redis-gunyu-checkpoint-hash)
    - [3.2 `checkpointName`](#32-checkpointname)
    - [3.3 Startup recovery in non-bisync mode](#33-startup-recovery-in-non-bisync-mode)
    - [3.4 Runtime updates in non-bisync mode](#34-runtime-updates-in-non-bisync-mode)
  - [4. Bisync checkpoint keys](#4-bisync-checkpoint-keys)
    - [4.1 Bisync namespace root: `checkpointName`](#41-bisync-namespace-root-checkpointname)
    - [4.2 `checkpointName:frontier`](#42-checkpointnamefrontier)
    - [4.3 `marker:{slotTag}`](#43-markerslottag)
    - [4.4 `latest:{slotTag}`](#44-latestslottag)
    - [4.5 `commit:{slotTag}:<unitSeq>`](#45-commitslottagunitseq)
    - [4.6 `index:{slotTag}`](#46-indexslottag)
    - [4.7 `rdb:{slotTag}:<unitSeq>`](#47-rdbslottagunitseq)
  - [5. Bisync sync: startup recovery and runtime updates](#5-bisync-sync-startup-recovery-and-runtime-updates)
    - [5.1 Startup recovery](#51-startup-recovery)
    - [5.2 Runtime updates](#52-runtime-updates)
  - [6. Bisync pipeline/parallel: startup recovery and runtime updates](#6-bisync-pipelineparallel-startup-recovery-and-runtime-updates)
    - [6.1 Startup recovery](#61-startup-recovery)
    - [6.2 Runtime updates](#62-runtime-updates)
  - [7. Checkpoint namespace creation and migration at startup](#7-checkpoint-namespace-creation-and-migration-at-startup)
  - [8. Deletion and GC](#8-deletion-and-gc)
  - [9. Key conclusions](#9-key-conclusions)

## 1. Purpose

This document describes the checkpoint-related Redis keys used by the current
`redis-GunYu` implementation. It focuses on the following questions:

- Which keys exist in non-bisync and bisync modes?
- What are the type, fields, and values of each key?
- How are checkpoints restored from these keys at startup?
- How are these keys updated at runtime?
- How are the keys related, and which state is the authoritative checkpoint?

The main implementation files covered by this document are:

- `config/var.go`
- `pkg/redis/checkpoint/checkpoint.go`
- `pkg/redis/checkpoint/checkpoint_info.go`
- `pkg/redis/checkpoint/bisync.go`
- `syncer/output.go`
- `syncer/syncer.go`
- `syncer/bisync.go`
- `syncer/bisync_rdb.go`

Additional notes:

- This document retains the checkpoint, frontier, and journal description for
  `parallel` because that internal path still exists in the implementation.
- Current performance results do not show a stable advantage for `parallel`.
  Prefer `sync` by default and evaluate `pipeline` for workloads that need more
  throughput.

## 2. Overview

Whether bisync is enabled or not, checkpoint-related keys have two broad layers:

1. Index layer

   A `runId -> checkpointName` mapping locates the checkpoint namespace for the
   current source.

   The fixed key is:

   ```text
   redis-gunyu-checkpoint-hash
   ```

2. Data layer

   The actual recovery-point data is stored in `checkpointName` and its derived
   keys.

The authoritative state differs by mode:

- Non-bisync: the recovery point is primarily `runId_offset` in the ordinary
  `checkpointName` hash.
- Bisync `sync`: the recovery point is primarily derived from each slot's
  `latest` record.
- Bisync `pipeline`/`parallel`: the recovery point is primarily derived from
  `frontier + commit journal`.

The constants are defined as follows:

```go
CheckpointKey        = "redis-gunyu-checkpoint"
CheckpointKeyHashKey = "redis-gunyu-checkpoint-hash"
```

## 3. Non-bisync checkpoint keys

Non-bisync refers to the ordinary one-way synchronization path where
`output.replay.bisyncEnabled` is not enabled.

### 3.1 `redis-gunyu-checkpoint-hash`

Key:

```text
redis-gunyu-checkpoint-hash
```

Type:

```text
HASH
```

Purpose:

- Stores the `runId -> checkpointName` mapping.
- During startup recovery, this key is used first to locate the checkpoint root
  for the current source run ID.

Example:

```redis
HSET redis-gunyu-checkpoint-hash r1 redis-gunyu-checkpoint
```

This means:

- The current source has `runId = r1`.
- Its checkpoint root key is `redis-gunyu-checkpoint`.

### 3.2 `checkpointName`

In non-bisync mode, `checkpointName` can have two forms:

- Standalone or ordinary cases:

  ```text
  redis-gunyu-checkpoint
  ```

- Cluster cases where the current transaction mode needs a key that belongs to
  an allowed target slot range:

  ```text
  redis-gunyu-checkpoint-xxxxx
  ```

  The suffix is selected at runtime so that the key falls within the allowed
  slot range.

Type:

```text
HASH
```

Field layout:

- `<runId>_runid`
- `<runId>_version`
- `<runId>_offset`
- `<runId>_mtime`

Example:

```redis
HSET redis-gunyu-checkpoint \
  r1_runid r1 \
  r1_version 1 \
  r1_offset 123456 \
  r1_mtime 1710000000000000000
```

Meaning:

- `r1_offset=123456` means that source `runId=r1` has been safely synchronized
  through offset `123456`.
- `mtime` is the most recent update time.

This `offset` is the central recovery point for the non-bisync path.

### 3.3 Startup recovery in non-bisync mode

The non-bisync startup recovery flow is:

1. Read the source's current `runId` or `runId/runId2`.
2. Look up the corresponding `checkpointName` in
   `redis-gunyu-checkpoint-hash`.
3. Read the `checkpointName` hash.
4. Find the `<runId>_offset` matching the current `runId`.
5. Use that offset as the incremental synchronization starting point.

Example Redis state:

```redis
HSET redis-gunyu-checkpoint-hash r1 redis-gunyu-checkpoint
HSET redis-gunyu-checkpoint \
  r1_runid r1 \
  r1_version 1 \
  r1_offset 123456 \
  r1_mtime 1710000000000000000
```

At startup:

1. The source reports the current `runId = r1`.
2. GunYu executes:

   ```redis
   HGET redis-gunyu-checkpoint-hash r1
   ```

   The result is:

   ```text
   redis-gunyu-checkpoint
   ```

3. GunYu then executes:

   ```redis
   HGETALL redis-gunyu-checkpoint
   ```

4. It parses:

   ```text
   r1_offset = 123456
   ```

5. Synchronization resumes from source offset `123456`.

Additional behavior:

- A standalone output scans multiple databases and selects the checkpoint with
  the greatest offset, preferring the newer `mtime` when necessary.
- A cluster output only reads DB 0.

### 3.4 Runtime updates in non-bisync mode

There are two categories of non-bisync updates.

#### 3.4.1 Update after full sync

After the complete RDB has been replayed, GunYu writes an ordinary checkpoint.

Example:

```redis
HSET redis-gunyu-checkpoint \
  r1_runid r1 \
  r1_version 1 \
  r1_offset 500000 \
  r1_mtime 1710001000000000000
```

This means:

- Full sync has been completely applied to the target.
- Subsequent incremental synchronization may begin at `offset=500000`.

#### 3.4.2 Updates during AOF replay

Ordinary AOF replay has two modes.

1. Non-transaction mode

   Business commands execute first. After their replies are received, the
   checkpoint offset is refreshed on a timer.

   Example:

   ```redis
   SET user:1 v1
   SET user:2 v2
   HSET redis-gunyu-checkpoint r1_offset 500123
   ```

   Note:

   - The checkpoint update and business commands do not share one transaction
     boundary.
   - Therefore, this can only be described as a recovery point that is kept as
     close as practical to the applied data.

2. Transaction mode

   Business commands and the checkpoint update are placed in the same
   `MULTI/EXEC` transaction.

   Example:

   ```redis
   MULTI
   SET user:1 v1
   SET user:2 v2
   HSET redis-gunyu-checkpoint r1_runid r1 r1_version 1
   HSET redis-gunyu-checkpoint r1_offset 500123
   EXEC
   ```

   This provides stronger semantics:

   - The business commands succeed.
   - The checkpoint advances to the corresponding offset in the same
     transaction.

## 4. Bisync checkpoint keys

Bisync still uses `redis-gunyu-checkpoint-hash` as its index layer, but expands
the data layer into a stable namespace.

A typical `checkpointName` looks like:

```text
redis-gunyu-checkpoint-bisync:aa11bb22cc33dd44ee55ff66
```

This is a stable namespace root, not a temporary shard key derived from a
single topology calculation.

### 4.1 Bisync namespace root: `checkpointName`

Example:

```text
redis-gunyu-checkpoint-bisync:aa11bb22cc33
```

Type:

```text
HASH
```

It stores two categories of data.

First, ordinary shared checkpoint fields:

- `<runId>_runid`
- `<runId>_version`
- `<runId>_offset`
- `<runId>_mtime`

Second, bisync namespace metadata:

- `bisync_mode`
- `bisync_mode_mtime`

Example:

```redis
HSET redis-gunyu-checkpoint-bisync:aa11bb22cc33 \
  r1_runid r1 \
  r1_version 1 \
  r1_offset 123456 \
  r1_mtime 1710000000000000000 \
  bisync_mode parallel \
  bisync_mode_mtime 1710000001000000000
```

The root key serves three purposes:

1. It provides a stable root name for the entire bisync namespace.
2. It records whether the namespace belongs to `sync`, `pipeline`, or
   `parallel` mode.
3. It retains a shared checkpoint offset as a barrier or migration seed.

However:

- During bisync AOF recovery, `runId_offset` in the root key is generally not
  the final authoritative recovery point.
- The authoritative recovery point comes from `latest` in `sync` mode and from
  `frontier + commit journal` in `pipeline`/`parallel` mode.

### 4.2 `checkpointName:frontier`

Key:

```text
<checkpointName>:frontier
```

Example:

```text
redis-gunyu-checkpoint-bisync:aa11bb22cc33:frontier
```

Type:

```text
HASH
```

Fields:

- `version`
- `run_id`
- `unit_seq`
- `end_offset`
- `mtime`

Example:

```redis
HSET redis-gunyu-checkpoint-bisync:aa11bb22cc33:frontier \
  version 1 \
  run_id r1 \
  unit_seq 88 \
  end_offset 123456 \
  mtime 1710000002000000000
```

Meaning:

- In `parallel` mode, units have been acknowledged continuously through
  `unit_seq=88`.
- This sequence corresponds to source offset `123456`.
- Every unit before this point has been authoritatively incorporated into the
  recovery surface.

`frontier` is a namespace-global key, not state private to one slot.

### 4.3 `marker:{slotTag}`

Key:

```text
redis-gunyu-bisync:<checkpointName>:marker:{slotTag}
```

Type:

```text
STRING
```

Value:

- A JSON string.
- Stored with a TTL.

Fields:

- `record_type`
- `version`
- `run_id`
- `syncer_id`
- `unit_seq`
- `start_offset`
- `end_offset`
- `slot`
- `digest`

Example:

```redis
SET redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11bb22cc33:marker:{slot-8338-x} \
  '{"version":"1","run_id":"r1","syncer_id":"syncer-a","unit_seq":9,"start_offset":10,"end_offset":20,"slot":8338,"digest":"deadbeef"}' \
  PX 86400000
```

Purpose:

- It is placed at the beginning of a mirrored transaction.
- When the reverse-side parser sees it, the parser knows that the transaction
  is a GunYu-mirrored transaction rather than an original business write.
- The complete transaction is therefore suppressed instead of being replayed
  in the reverse direction.

The `marker` is responsible for loop suppression. It is not a recovery point.

### 4.4 `latest:{slotTag}`

Key:

```text
redis-gunyu-bisync:<checkpointName>:latest:{slotTag}
```

Type:

```text
HASH
```

Fields:

- `version`
- `run_id`
- `syncer_id`
- `unit_seq`
- `start_offset`
- `end_offset`
- `slot`
- `digest`
- `mtime`

Example:

```redis
HSET redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11bb22cc33:latest:{slot-8338-x} \
  version 1 \
  run_id r1 \
  syncer_id syncer-a \
  unit_seq 9 \
  start_offset 10 \
  end_offset 20 \
  slot 8338 \
  digest deadbeef \
  mtime 1710000003000000000
```

Meaning:

- For `slot=8338`, the latest confirmed replay unit is `unit_seq=9`.
- It covers the source offset range `[10, 20]`.
- During recovery, that slot's committed position can be treated as
  `offset=20`.

`latest` is an authoritative recovery source only in bisync `sync` mode.

### 4.5 `commit:{slotTag}:<unitSeq>`

Key:

```text
redis-gunyu-bisync:<checkpointName>:commit:{slotTag}:<unitSeq>
```

Example:

```text
redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11bb22cc33:commit:{slot-8338-x}:00000000000000000009
```

Type:

```text
HASH
```

Fields:

- `version`
- `run_id`
- `syncer_id`
- `unit_seq`
- `start_offset`
- `end_offset`
- `slot`
- `digest`
- `mtime`

Example:

```redis
HSET redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11bb22cc33:commit:{slot-8338-x}:00000000000000000009 \
  version 1 \
  run_id r1 \
  syncer_id syncer-a \
  unit_seq 9 \
  start_offset 10 \
  end_offset 20 \
  slot 8338 \
  digest deadbeef \
  mtime 1710000003000000000
```

This key records:

- That one replay unit has been committed successfully.
- The source run ID to which it belongs.
- The offset range it covers.
- The slot to which it was routed.
- Its monotonic sequence number, `unit_seq`, within the namespace.

It is a journal record for `pipeline`/`parallel` mode.

Important:

- A `commit` is not the current global latest position.
- A `commit` only proves that one unit has been committed.
- It must be combined with `frontier` to form an authoritative recovery point.

### 4.6 `index:{slotTag}`

Key:

```text
redis-gunyu-bisync:<checkpointName>:index:{slotTag}
```

Type:

```text
ZSET
```

Score:

- `unit_seq`

Member:

- The complete name of the corresponding `commit` key.

Example:

```redis
ZADD redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11bb22cc33:index:{slot-8338-x} \
  9 redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11bb22cc33:commit:{slot-8338-x}:00000000000000000009
```

Purpose:

- Provides an ordered `unit_seq` index over the commit journal in
  `pipeline`/`parallel` mode.
- During startup recovery, GunYu first finds candidate `commit` keys through
  `index`, then reads the corresponding hashes.

The `index` does not store a recovery point; it only stores index relationships.

### 4.7 `rdb:{slotTag}:<unitSeq>`

Key format:

```text
redis-gunyu-bisync:<checkpointName>:rdb:{slotTag}:<unitSeq>
```

Current status:

- This key format is defined in the code.
- The parser recognizes it.
- The current primary write path does not actually persist this key.

The current RDB bisync path writes only:

- `marker`
- Business commands

It does not write a separate `rdb record`.

For the current production recovery behavior, treat this key as reserved and
supported for compatible parsing, not as part of the primary checkpoint data.

## 5. Bisync sync: startup recovery and runtime updates

The core characteristics of `sync` mode are:

- Each slot retains one `latest` record.
- No commit journal is retained.
- Recovery scans `latest` across all slots and selects the most suitable
  recovery point.

### 5.1 Startup recovery

Assume Redis contains:

```redis
HSET redis-gunyu-checkpoint-hash r1 redis-gunyu-checkpoint-bisync:aa11
HSET redis-gunyu-checkpoint-bisync:aa11 bisync_mode sync

HSET redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11:latest:{slot-a} \
  version 1 run_id r1 syncer_id syncer-a unit_seq 7 start_offset 81 end_offset 100 slot 100 digest d1 mtime 10

HSET redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11:latest:{slot-b} \
  version 1 run_id r1 syncer_id syncer-a unit_seq 8 start_offset 101 end_offset 120 slot 200 digest d2 mtime 11

HSET redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11:latest:{slot-c} \
  version 1 run_id r1 syncer_id syncer-a unit_seq 6 start_offset 61 end_offset 90 slot 300 digest d3 mtime 9
```

Startup recovery proceeds as follows:

1. The source reports its current `runId = r1`.
2. GunYu looks up `redis-gunyu-checkpoint-hash` and finds:

   ```text
   r1 -> redis-gunyu-checkpoint-bisync:aa11
   ```

3. Using `checkpointName=redis-gunyu-checkpoint-bisync:aa11`, GunYu scans the
   `latest:{slotTag}` key for every recovery slot.
4. It retains only records whose `run_id` matches `r1`.
5. It selects the record with:

   - The greatest `end_offset`.
   - The newer `mtime` if offsets are equal.

6. From the three records above, it selects:

   ```text
   latest:{slot-b}
   end_offset = 120
   ```

7. The startup recovery point is therefore:

   ```text
   run_id = r1
   offset = 120
   ```

In `sync` mode, the authoritative recovery point is not `r1_offset` in the root
key. It is the best record selected by scanning `latest`.

### 5.2 Runtime updates

Assume a new replay unit has:

- `unit_seq = 9`
- `slot = 8338`
- `start_offset = 121`
- `end_offset = 140`
- One business command:

  ```redis
  SET foo{slot-8338-x} value
  ```

`sync` mode wraps it in a real transaction:

```redis
MULTI
SET redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11:marker:{slot-8338-x} \
  '{"version":"1","run_id":"r1","syncer_id":"syncer-a","unit_seq":9,"start_offset":121,"end_offset":140,"slot":8338,"digest":"deadbeef"}' \
  PX 86400000
SET foo{slot-8338-x} value
HSET redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11:latest:{slot-8338-x} \
  version 1 \
  run_id r1 \
  syncer_id syncer-a \
  unit_seq 9 \
  start_offset 121 \
  end_offset 140 \
  slot 8338 \
  digest deadbeef \
  mtime 1710000010000000000
EXEC
```

After the transaction commits successfully:

- `marker` tells the reverse parser that this is a mirrored transaction.
- The business command has been applied successfully.
- `latest:{slot-8338-x}` becomes the latest authoritative checkpoint for that
  slot.

On the next restart, if this `latest` record has the greatest offset across all
slots, recovery starts directly at `offset=140`.

## 6. Bisync pipeline/parallel: startup recovery and runtime updates

The core characteristics of `pipeline`/`parallel` mode are:

- A `commit journal` is written as units commit.
- `index` provides an index over the journal.
- `frontier` represents the global recovery frontier that is known to be
  contiguous and closed.
- Recovery does not simply select the greatest `unit_seq`. It extends forward
  contiguously from the current `frontier`.

### 6.1 Startup recovery

Consider a simple example. Redis contains:

```redis
HSET redis-gunyu-checkpoint-hash r1 redis-gunyu-checkpoint-bisync:aa11
HSET redis-gunyu-checkpoint-bisync:aa11 bisync_mode parallel

HSET redis-gunyu-checkpoint-bisync:aa11:frontier \
  version 1 \
  run_id r1 \
  unit_seq 9 \
  end_offset 321 \
  mtime 456

HSET redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11:commit:{slot-1}:00000000000000000010 \
  version 1 \
  run_id r1 \
  syncer_id syncer-a \
  unit_seq 10 \
  start_offset 322 \
  end_offset 400 \
  slot 1 \
  digest d10 \
  mtime 457

ZADD redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11:index:{slot-1} \
  10 redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11:commit:{slot-1}:00000000000000000010
```

Startup recovery proceeds as follows:

1. The source reports its current `runId = r1`.
2. GunYu finds `checkpointName=redis-gunyu-checkpoint-bisync:aa11` through
   `redis-gunyu-checkpoint-hash`.
3. It reads:

   ```text
   redis-gunyu-checkpoint-bisync:aa11:frontier
   ```

   The result is:

   ```text
   frontier.unit_seq = 9
   frontier.end_offset = 321
   ```

4. It calculates:

   ```text
   minSeq = frontier.unit_seq + 1 = 10
   ```

5. It scans every slot's `index:{slotTag}` with logic equivalent to:

   ```redis
   ZRANGEBYSCORE <indexKey> 10 +inf
   ```

6. This returns candidate `commit` keys.
7. GunYu executes `HGETALL` for those `commit` keys.
8. It reconstructs a contiguous frontier in `unit_seq` order.

   In this example:

   - The current frontier has reached `seq=9`.
   - `seq=10` exists.
   - There is no gap.

9. The frontier can advance to:

   ```text
   unit_seq = 10
   end_offset = 400
   ```

10. The final startup recovery point is:

    ```text
    run_id = r1
    offset = 400
    ```

Now consider an example with a gap. If Redis only contains:

```text
frontier.unit_seq = 8
commit(seq=10) exists
commit(seq=9) does not exist
```

Then startup recovery:

- Must not jump directly to `seq=10`.
- Must remain at the offset represented by `frontier(seq=8)`.

The reason is:

- `pipeline`/`parallel` recognizes only the journal sequence that is contiguous
  after the frontier.
- A greater `unit_seq` cannot be used to skip over an intermediate gap.

This is the central semantic rule of `frontier + commit journal`.

### 6.2 Runtime updates

Assume a new replay unit has:

- `unit_seq = 9`
- `slot = 8338`
- `start_offset = 121`
- `end_offset = 140`
- Business command:

  ```redis
  SET foo{slot-8338-x} value
  ```

`pipeline`/`parallel` first commits a real transaction:

```redis
MULTI
SET redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11:marker:{slot-8338-x} \
  '{"version":"1","run_id":"r1","syncer_id":"syncer-a","unit_seq":9,"start_offset":121,"end_offset":140,"slot":8338,"digest":"deadbeef"}' \
  PX 86400000
SET foo{slot-8338-x} value
HSET redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11:commit:{slot-8338-x}:00000000000000000009 \
  version 1 \
  run_id r1 \
  syncer_id syncer-a \
  unit_seq 9 \
  start_offset 121 \
  end_offset 140 \
  slot 8338 \
  digest deadbeef \
  mtime 1710000010000000000
ZADD redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11:index:{slot-8338-x} \
  9 \
  redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11:commit:{slot-8338-x}:00000000000000000009
EXEC
```

After this transaction commits:

- The `commit` exists.
- The `index` entry exists.
- The `frontier` does not necessarily advance immediately.

This is because:

- `pipeline`/`parallel` allows multiple units to be in flight.
- The coordinator advances the recovery surface serially.
- The coordinator advances the frontier only when
  `frontier.unit_seq + 1` is reachable contiguously.

For example, assume the current state is:

```text
frontier = seq 8, offset 120
```

If `unit 10` completes before `unit 9`, Redis first contains:

```text
commit(seq=10)
index(score=10 -> commitKey10)
```

But `frontier` remains:

```text
seq = 8
offset = 120
```

Only after `unit 9` also completes can the coordinator:

1. Incorporate both `seq=9` and `seq=10` into the contiguous frontier.
2. Update `checkpointName:frontier`.
3. Delete the `commit` keys absorbed by the frontier.
4. Execute `ZREM` on their corresponding `index` entries.

The resulting update may look like:

```redis
HSET redis-gunyu-checkpoint-bisync:aa11:frontier \
  version 1 \
  run_id r1 \
  unit_seq 10 \
  end_offset 160 \
  mtime 1710000011000000000

DEL redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11:commit:{slot-8338-x}:00000000000000000009
DEL redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11:commit:{slot-8338-x}:00000000000000000010
ZREM redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11:index:{slot-8338-x} <commitKey9> <commitKey10>
```

The `pipeline`/`parallel` recovery surface therefore advances in two layers:

1. Commit layer: write `commit + index` first.
2. Convergence layer: let `frontier` absorb the contiguous portion.

## 7. Checkpoint namespace creation and migration at startup

At bisync startup, GunYu first determines which `checkpointName` belongs to the
current run ID.

The flow is:

1. Look up the source's current `runId/runId2` in
   `redis-gunyu-checkpoint-hash`.
2. If a `checkpointName` is found:

   - Reuse that namespace directly.
   - Read `bisync_mode` from the root key.

3. If no `checkpointName` is found:

   - Create `redis-gunyu-checkpoint-bisync:<random>`.
   - Write the `runId -> checkpointName` mapping.
   - Write `bisync_mode` to the root key.

4. If an old namespace is found but its mode differs from the expected mode:

   - Extract an authoritative seed from the old namespace.
   - Generate a new `checkpointName`.
   - Write the minimum recovery state into the new namespace.
   - Redirect `checkpoint-hash` to the new namespace.
   - Attempt to clean up the old namespace.

Two migration examples:

1. `sync -> pipeline|parallel`

   - Scan `latest` in the old namespace.
   - Select the best `latest` record.
   - Convert it into a seed.
   - Write the root checkpoint and `frontier` in the new namespace.

2. `pipeline|parallel -> sync`

   - Read the old `frontier` first.
   - Read the commit journal after the frontier.
   - Reconstruct the contiguous frontier.
   - Convert the frontier into a seed.
   - Write the root checkpoint and one `latest` record in the new namespace.

Therefore:

- `checkpoint-hash` is the entry-point index.
- `checkpointName` is the stable namespace root.
- `latest` and `frontier + commit journal` are the mode-specific authoritative
  states.

## 8. Deletion and GC

Checkpoint deletion has three main categories.

### 8.1 Deleting ordinary checkpoint fields

The ordinary `DelCheckpoint` operation removes the following fields from the
corresponding `checkpointName` hash:

- `<runId>_runid`
- `<runId>_offset`
- `<runId>_version`
- `<runId>_mtime`

This deletion path is primarily used for non-bisync checkpoint fields or shared
root checkpoint fields.

### 8.2 Periodic stale checkpoint GC

The general GC flow is:

1. Collect the run IDs that are still active on the input side.
2. Scan `redis-gunyu-checkpoint-hash`.
3. For each `runId -> checkpointName` mapping:

   - If the run ID still exists, delete stale copies while retaining the newest
     copy where possible.
   - If the run ID no longer exists and every copy of its checkpoint has become
     empty, delete the mapping from `checkpoint-hash`.

This GC primarily operates on ordinary checkpoint roots. It is not responsible
for generally scanning every derived bisync key.

### 8.3 Explicit cleanup after bisync namespace migration

After a bisync mode migration detaches the old namespace from
`checkpoint-hash`, ordinary stale GC may no longer be able to find it through a
run ID.

The implementation therefore performs an explicit best-effort cleanup of:

- The root key: `checkpointName`
- The frontier key: `checkpointName:frontier`
- Per-slot `marker` keys
- Per-slot `latest` keys
- Per-slot `index` keys
- Every `commit` key referenced by indexes in `pipeline`/`parallel` mode

This prevents an unreachable old bisync namespace from remaining after mode
migration.

## 9. Key conclusions

The current implementation can be summarized in three parts.

1. Non-bisync

   - The checkpoint is stored primarily in the `checkpointName` hash.
   - The authoritative recovery point is `<runId>_offset`.

2. Bisync `sync`

   - The root key owns the namespace and mode metadata.
   - The authoritative recovery point comes from each slot's `latest` record.

3. Bisync `pipeline`/`parallel`

   - `commit` only indicates that one unit has been committed.
   - `index` only indexes the journal.
   - `frontier` represents the current global recovery frontier that has been
     closed contiguously.
   - The authoritative recovery point is the result of contiguously
     reconstructing `frontier + commit journal`.

Looking at only one checkpoint key can easily lead to an incorrect
interpretation of recovery semantics.

To understand the current code correctly:

- First distinguish non-bisync from bisync.
- Within bisync, distinguish `sync`, `pipeline`, and `parallel`.
- Finally determine whether a key is an index, loop-suppression control data, or
  authoritative recovery-surface data.
