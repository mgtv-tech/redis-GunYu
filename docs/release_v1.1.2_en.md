# redis-GunYu v1.1.2 Release Notes

## 1. Overview

`v1.1.2` is a patch release on top of `v1.1.1`, focused on fixing incorrect `SELECT` command encoding for multi-digit Redis DB indexes in standalone sync scenarios.

This version does not introduce new feature switches or configuration format changes. Its primary goal is to fix incremental replay and resume failures when `db >= 10`, and it is intended as a direct upgrade from `v1.1.1`.

## 2. Highlights

### 2.1 Fixed AOF Replay for Multi-Digit DB Indexes

- Fixed the DB index encoding used when rebuilding `SELECT` commands in the output-side AOF replay path
- The previous implementation only worked for `db 0..9`; when the target DB was `10` or higher, the DB index could be encoded as a single incorrect byte
- In standalone-to-standalone sync, this could cause the target Redis to return `ERR value is not an integer or out of range`

### 2.2 Fixed Resume-From-Breakpoint for Multi-Digit DB Indexes

- Fixed the same encoding issue when restoring the starting DB during `resumeFromBreakPoint`
- When the checkpoint DB is `10` or higher, sync can now resume on the correct DB instead of failing on the first `SELECT`

### 2.3 Added Regression Coverage

- Added an AOF replay regression test for `SELECT 10`
- Added a regression test for resume startup with a multi-digit DB index
- Added a regression test for `targetDbMap` remapping to a multi-digit target DB

## 3. Scope of Impact

The affected cases are primarily:

- standalone Redis as the output target
- AOF incremental replay that reaches `SELECT 10` or higher
- resume flow restoring to `db >= 10`
- `targetDb` / `targetDbMap` remapping into `db >= 10`

The following cases are typically unaffected:

- standalone sync using only `db 0..9`
- Redis Cluster as the output target
- the RDB full-replay path

## 4. Compatibility and Upgrade Notes

- `v1.1.2` does not introduce configuration-breaking changes
- Existing `v1.1.1` configurations can be reused directly
- Regression coverage has been added for `db 10+` scenarios, so no parameter changes are required after upgrade

If incremental sync was interrupted before the fix, validate the following after upgrade:

- whether the syncer continues advancing offsets
- whether writes to target `db 10+` resume correctly
- whether `ERR value is not an integer or out of range` no longer appears in logs

## 5. Recommended Upgrade Scenarios

If you are currently using `v1.1.1`, upgrading to `v1.1.2` is recommended when any of the following applies:

- your workload uses Redis logical DB `10` or higher
- `resumeFromBreakPoint` is enabled
- you use `targetDb` or `targetDbMap` for DB remapping

## 6. Related Issue

- Issue: `#108` `When the Redis sync DB index is greater than 10, data is no longer replicated`
