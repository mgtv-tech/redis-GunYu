# redis-GunYu v1.13 Release Notes

## 1. Overview

`v1.13` is a feature release on top of `v1.1.2`. Its main addition is a configurable channel backend with a new `memory channel` implementation.

This release does not change the existing `input -> channel -> output` sync pipeline. Instead, it extends the local cache layer with a new backend option:

- `storer`: the original disk-backed cache, still the default
- `memory`: a new in-memory cache that reduces disk I/O

If your deployment is more sensitive to local write amplification and latency, and you can accept non-persistent local cache across process restarts, `v1.13` is the version intended for that tradeoff.

## 2. Highlights

### 2.1 Added the memory channel backend

- Added `channel.type: memory`
- Supports RDB buffering for full sync
- Supports AOF buffering for incremental sync
- Keeps the existing `Channel` abstraction unchanged, so upper-layer sync logic remains the same

The `memory channel` stores RDB/AOF data in segmented in-memory buffers instead of local disk files.

### 2.2 Added dynamic channel backend selection

- `channel.type` now supports both `storer` and `memory`
- The default remains `storer`, so existing configs keep the same behavior
- Config validation and runtime initialization now select the backend implementation dynamically

This keeps upgrades conservative: existing deployments do not switch to the new backend unless configured explicitly.

### 2.3 Added hard capacity control for memory channel

- Added `channel.memory.maxSize`
- Added `channel.memory.logSize`
- Old segments are reclaimed when the cache reaches the `maxSize` limit and becomes reclaimable
- The memory ceiling is a hard limit rather than an advisory target

The design goal here is to reduce disk I/O while keeping memory usage bounded and predictable.

### 2.4 Added tests and implementation documentation

- Added `syncer/memory_channel_test.go`
- Covered core behavior including RDB/AOF read-write flow, offset ranges, and hard max-size enforcement
- Added `docs/memory_channel_impl_zh.md` to document the internal structure, concurrency model, GC behavior, and limitations
- Updated the Chinese and English sync configuration docs with `channel.type: memory`

## 3. Configuration Changes

### 3.1 New channel type configuration

```yaml
channel:
  type: memory
  memory:
    maxSize: 536870912
    logSize: 104857600
```

New configuration items:

- `channel.type`
  - `storer`: disk-backed cache, default
  - `memory`: in-memory cache
- `channel.memory.maxSize`
  - maximum in-memory cache size
  - default: `512 MiB`
- `channel.memory.logSize`
  - logical in-memory segment size
  - default: `100 MiB`

### 3.2 Backward compatibility for existing configs

- If `channel.type` is omitted, `storer` is still used
- The existing `channel.storer` structure is unchanged
- Existing `v1.1.2` configs can be reused directly

## 4. Recommended Use Cases and Limits

### 4.1 Recommended use cases for memory channel

- Local disk I/O is a primary bottleneck in the sync path
- You want to reduce local RDB/AOF persistence overhead
- Lower-latency cache flow matters more than local persistence
- Volatile channel cache is acceptable for the workload

### 4.2 Current limitations

- `memory channel` does not preserve local cache across process restarts
- If `maxSize` is too small for a long replay window, older offsets may become unavailable
- Once early RDB segments are reclaimed, that RDB is no longer replayable as a complete snapshot
- It is not a replacement for large-capacity, long-retention persistent local backlog storage

In practice, `memory channel` is meant for reducing local write pressure with bounded memory usage, not for replacing the full persistence semantics of `storer`.

## 5. Compatibility and Upgrade Notes

- `v1.13` does not change the default channel behavior
- If you do not update the config, the system continues to run with the `storer` backend
- After upgrading to `v1.13`, you can switch to `memory` mode gradually where it makes sense
- Before enabling `memory`, size `maxSize` against expected peak traffic and replay window

After upgrade, validate at least the following:

- both full sync and incremental sync continue to advance normally
- offsets keep moving forward under `memory channel`
- memory usage stays within the expected `maxSize` envelope
- restart behavior matches your operational expectations

## 6. Recommended Upgrade Scenarios

If you are currently on `v1.1.2`, consider upgrading to `v1.13` when any of the following applies:

- you need to reduce local disk write pressure
- your current sync latency is correlated with local cache persistence overhead
- you want to switch cache backends without changing the existing sync pipeline
- you need the flexibility to choose between `storer` and `memory` per scenario

If your deployment depends on cache persistence across restarts, or on a longer local backlog window, the default `storer` mode is still the safer choice.

## 7. Related Documentation

- Configuration guide: [sync_configuration_en.md](./sync_configuration_en.md)
- Implementation note: [memory_channel_impl_zh.md](./memory_channel_impl_zh.md)
