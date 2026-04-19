# redis-GunYu v1.1.0 Release Notes

## 1. Overview

`v1.1.0` introduces `bisync`, the new bidirectional synchronization capability in `redis-GunYu`, for running `A -> B` and `B -> A` sync links between two Redis deployments.

This release does not turn `redis-GunYu` into a strongly consistent multi-master database. Its goal is to provide a recoverable, observable, and rollout-friendly bidirectional sync solution for workloads that can accept eventual consistency.

## 2. Highlights

- Added `bisync` bidirectional synchronization for both Redis standalone and Redis cluster
- Supports both AOF incremental sync and RDB full-sync replay paths
- Commits each replay unit with a real `MULTI/EXEC` transaction, together with business commands, loop-suppression markers, and recovery metadata
- Supports recovery after syncer restart, Redis failover, and target cluster topology changes
- Added unified `output.replay.mode` configuration for replay execution semantics
- Legacy `enableAofPipeline` is still accepted for backward-compatible loading, but new configurations should migrate to `mode`
- Added bisync design notes, implementation notes, test design, performance reports, and operational documentation

## 3. Configuration and Upgrade Notes

### 3.1 Key Configuration

Bidirectional synchronization is configured under `output.replay`:

```yaml
output:
  replay:
    resumeFromBreakPoint: true
    replayTransaction: true
    bisyncEnabled: true
    mode: sync
    keyExists: replace
```

Key points:

- `bisyncEnabled` is the only explicit switch for bisync
- `replayTransaction` is still recommended, but it is not the bisync switch
- `mode` is the unified configuration for AOF replay execution semantics; the public release scope of this version includes `sync` and `pipeline`
- For consistency-sensitive production workloads, `mode: sync` is the recommended default

### 3.2 Compatibility Notes

- Legacy `enableAofPipeline` is still accepted for backward-compatible loading
- New configurations should migrate to `output.replay.mode`
- When moving from one-way sync to bidirectional sync, review filters, control-plane key namespaces, same-key write semantics, and rollback procedures separately

## 4. Usage Recommendations

- Deploy syncers in both directions, and enable `bisyncEnabled: true` on both sides
- Start rollout with a limited business prefix, slot range, or controlled traffic scope
- Use `mode: sync` as the default production choice
- If you plan to use `mode: pipeline`, validate recovery, failover, and soak behavior before production rollout
- Define business conflict semantics for same-key writes on both sides before enabling bisync

## 5. Known Limitations

### 5.1 Consistency and Conflict Semantics

- Bisync does not resolve business conflicts
- If both sides write the same key, the final result depends on Redis command semantics, replay order, and recovery behavior
- Non-idempotent commands such as `INCR`, `LPUSH`, and `XADD` should be reviewed at the business level before rollout

### 5.2 Cluster Constraints

- In cluster mode, each replay unit must be provably bound to a single slot
- Cross-slot source transactions are conservatively rejected instead of being partially replayed

### 5.3 Command Support Scope

- RedisJSON, RedisBloom, and similar module commands are not part of the stable release gate yet
- Commands whose key set cannot be proven through keyspec or `COMMAND GETKEYS` are not recommended for production bisync traffic

### 5.4 Control-Plane Keys

The following namespaces are reserved for GunYu control-plane metadata and must not be read, written, migrated, deleted, or reused by business traffic:

- `redis-gunyu-bisync:*`
- `redis-gunyu-checkpoint*`
- `/redis-gunyu*`

## 6. Release and Rollout Guidance

### 6.1 Before Release

- Complete bisync release validation against the production Redis version
- Confirm that the business command set does not include unverified module commands or commands whose keys cannot be parsed
- Confirm that operations, monitoring, and rollback procedures are ready

### 6.2 Canary Rollout

- Start with a limited business prefix, slot range, or traffic scope
- Watch syncer status, bisync metrics, Redis resource usage, and business-side data comparison results
- Pay close attention to `bisync_txn_commit`, `bisync_txn_suppress`, `bisync_single_slot_fail`, and `bisync_commit_backlog`

### 6.3 Rollback Guidance

If risks appear during rollout, roll back quickly by:

- disabling `bisyncEnabled`
- stopping the reverse link and keeping only one-way synchronization
- following the business-side reconciliation and repair plan

## 7. Related Documents

- [bisync_en.md](./bisync_en.md)
- [bisync.md](./bisync.md)
- [bisync_scheme_selection_en.md](./bisync_scheme_selection_en.md)
- [bisync_scheme1_impl_en.md](./bisync_scheme1_impl_en.md)
- [sync_configuration_en.md](./sync_configuration_en.md)
- [bisync_perf_report.md](./bisync_perf_report.md)
