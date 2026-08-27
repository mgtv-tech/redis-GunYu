# Redis Modules Support Design

- [1. Background And Goals](#1-background-and-goals)
- [2. Support Scope](#2-support-scope)
- [3. Incremental Module Command Sync](#3-incremental-module-command-sync)
- [4. Full RDB Sync](#4-full-rdb-sync)
- [5. Handling In Bisync](#5-handling-in-bisync)
- [6. Key Code Paths](#6-key-code-paths)
- [7. Validation](#7-validation)
- [8. Current Limitations](#8-current-limitations)
- [9. Production Notes](#9-production-notes)
- [10. Follow-up Recommendations](#10-follow-up-recommendations)

## 1. Background And Goals

Redis Modules such as RedisJSON, RediSearch, and RedisBloom introduce two synchronization problems:

- During AOF / incremental command sync, GunYu needs to know which keys a command touches so it can route correctly in cluster and bisync scenarios.
- During RDB / full sync, module-defined data types are stored as Redis module objects in binary RDB form, so GunYu cannot expand them like built-in string/hash/list types.

The goals of this work are:

- Add static keyspec coverage for validated module commands so incremental sync remains routable, filterable, and verifiable.
- Replay RDB keyspace module objects through opaque `RESTORE` without parsing module internals.
- Make the limitation around `MODULE_AUX` global module metadata such as RediSearch indexes explicit, so index loss is never silent.

## 2. Support Scope

Redis Modules are currently **partially supported**. This should not be interpreted as full support for every module command and every module data type. This document uses four support levels:

| Support Level | Meaning | Production Prerequisite |
| --- | --- | --- |
| Static keyspec covered | GunYu can extract the command's key set for routing, filtering, and bisync slot binding | The target Redis still needs the corresponding module, and the real business commands still need end-to-end verification |
| Incremental end-to-end validated | The command path has been exercised on Redis Stack and the target state was confirmed to converge | Validation covers only the command shapes listed here and the Redis Stack module combination used in testing |
| RDB keyspace object validated | Module keyspace data can be restored through opaque `RESTORE` | The target must load compatible modules and keep `replayRdbEnableRestore: true` |
| Unsupported / not promised | Missing keyspec, missing end-to-end verification, dependent on `MODULE_AUX`, target lacks the module, or module formats are incompatible | Requires more implementation, validation samples, or an external rebuild path |

The currently validated module command support matrix is:

| Module | Commands | Keyspec / Routing | Incremental End-to-End | Notes |
| --- | --- | --- | --- | --- |
| RedisJSON | `JSON.SET`, `JSON.DEL`, `JSON.MSET` | Supported | Passed | `tests/nonbisync/run_category11.sh` verified target JSON convergence after source writes |
| RedisBloom | `BF.ADD` | Supported | Passed | `tests/nonbisync/run_category11.sh` verified Bloom membership convergence |
| RedisBloom | `TDIGEST.MERGE` | Supported | Passed in an additional probe | Keyspec matches Redis Stack and treats only the destination digest as a key |
| RedisBloom | `TOPK.ADD` | Supported | Passed in an additional probe | Depends on a pre-created topk key such as one created by `TOPK.RESERVE` |
| RedisBloom | `CMS.MERGE` | Supported | Not promoted to a stable end-to-end result | Keyspec matches Redis Stack and treats only the destination sketch as a key; the extra probe only confirmed sync of prerequisite `CMS.INITBYDIM` / `CMS.INCRBY` source keys, and `CMS.MERGE` itself is not yet part of the stable release gate |
| RediSearch | `FT.CREATE`, `FT.DROPINDEX` | Supported | Passed | `tests/nonbisync/run_category11.sh` verified index create and drop replay |
| RediSearch | `FT.SEARCH` | Supported | Not applicable | This is a read command and normally does not enter AOF write replay; the keyspec is mainly useful for routing and verification tooling |

The codebase also adds static keyspec coverage for more RedisJSON and RedisBloom commands such as `JSON.ARRAPPEND`, `JSON.NUMINCRBY`, `BF.MADD`, `CMS.INCRBY`, `TDIGEST.ADD`, and `TOPK.RESERVE`. GunYu can parse keys for them, but **they are not all part of the stable end-to-end validation matrix in this document**. In production, append your real business commands to `keyspec_verify --samples-file` and run a source-to-target convergence test.

The currently validated RDB module object restore coverage is:

| Module Data | RDB Replay Method | Status | Notes |
| --- | --- | --- | --- |
| RedisJSON keyspace object | Raw-dump `RESTORE` | Validated on Redis Stack | GunYu does not parse JSON internals; the target ReJSON module loads the payload |
| RedisBloom keyspace object | Raw-dump `RESTORE` | Validated on Redis Stack | GunYu does not parse Bloom/CMS/TDigest/TopK internals; the target `bf` module loads the payload |
| RediSearch index metadata | `MODULE_AUX` | Full restore unsupported | The index is global module metadata rather than a normal keyspace object, so current handling can only fail or skip |
| Other Redis Modules | Undefined | Not promised | They require command keyspec confirmation, RDB payload validation, target module compatibility checks, and end-to-end replay results |

### 2.1 Explicitly Unsupported Or Not Promised

- Module commands that are not covered by static keyspec or have not passed `keyspec_verify` are not promised. GunYu would not be able to derive their key sets reliably, which is unsafe in cluster, bisync, and filtering paths.
- Module commands that only have static keyspec in code but no business-level end-to-end validation should be treated as "key parsing available", not "business semantics validated".
- RediSearch index full restore from RDB is unsupported. The reason is that index metadata is written as global `MODULE_AUX` data rather than as keyspace module objects, so it cannot be restored via single-key `RESTORE`.
- A target Redis that does not load the corresponding modules is unsupported. GunYu can only replay commands or `RESTORE` payloads; it cannot emulate the module implementation on the target side.
- Incompatible source and target module versions are unsupported. Module object RDB payloads are private module formats, and GunYu intentionally does not parse or transform them.
- `RdbTypeModule` type 1 is unsupported. Modern Redis module RDB payloads primarily use `RdbTypeModule2`, and the current implementation stops on type 1.

## 3. Incremental Module Command Sync

### 3.1 Why Keyspec Is Required

GunYu must know the true key set of a command in cluster and bisync scenarios for several reasons:

- Select the target Redis cluster node.
- Detect whether a multi-key command crosses slots.
- Apply key and slot filtering.
- Bind slot-local markers and recovery metadata inside bisync replay units.

Built-in Redis commands can rely on the project's static keyspec table. If a module command has no static keyspec, runtime handling would have to rely on the target Redis `COMMAND GETKEYS`. That creates two problems:

- If the target Redis does not load the module, `COMMAND GETKEYS` returns unknown command.
- Some paths need strict routing decisions before sending the command, not after a failure.

That is why validated module commands were added to `pkg/redis/keyspec/keyspec.go`.

It is important to keep one boundary clear: **keyspec support does not mean full business semantic support**. Keyspec only answers "which keys does this command touch" for routing, filtering, and verification. Whether real-time replay is truly usable still depends on:

- Whether the source actually emits that module command or an equivalent write into the replication stream.
- Whether the target has the corresponding module loaded.
- Whether the target module version accepts the command arguments and existing target-side data structures.
- Whether source and destination objects for multi-key module commands are already consistent on the target.
- Whether the command has been validated in an end-to-end convergence test.

For that reason, this document marks "keyspec covered" and "incremental end-to-end validated" separately. When a new module command is added, it should come with static keyspec, a `keyspec_verify` sample, and real Redis Stack replay validation.

### 3.2 Static Keyspec Rules

RedisJSON and most RedisBloom commands use the first argument as the key, for example:

```text
JSON.SET doc{t} $ {"a":1}
BF.ADD bf{t} item
TOPK.ADD topk{t} item
```

These commands use `genericKeyPos`.

`JSON.MSET` places one key every three arguments, at positions 1, 4, 7, and so on:

```text
JSON.MSET doc1{t} $ {"a":1} doc2{t} $ {"b":2}
```

So it uses `{1, -1, 3}`.

`CMS.MERGE` and `TDIGEST.MERGE` need special handling. On real Redis Stack, `COMMAND GETKEYS` returns only the destination key:

```text
CMS.MERGE dst{t} 2 src1{t} src2{t}
TDIGEST.MERGE dst{t} 2 src1{t} src2{t}
```

The reply is:

```text
dst{t}
```

So GunYu also treats only the first argument as a key. The source sketch or digest arguments must not be misclassified as keys, otherwise false cross-slot detection would be introduced. This matches Redis Stack `COMMAND GETKEYS`, but it also means source-object consistency for those commands must be guaranteed by prior synchronization; keyspec alone does not prove that the source objects exist or carry the expected contents.

RediSearch currently uses the index name as the key:

```text
FT.CREATE idx{t} ...
FT.SEARCH idx{t} ...
FT.DROPINDEX idx{t}
```

That lets cluster routing treat the index name as the command key.

### 3.3 Verifier

`tests/bisync/cmd/keyspec_verify` compares GunYu's static keyspec with Redis `COMMAND GETKEYS`.

Its main result categories are:

- `ok`: the static key set matches the Redis-reported key set.
- `unsupported`: the target Redis does not support the command, usually because the module is not loaded.
- `mismatch`: the static key set differs from Redis.
- `unresolved`: Redis can resolve the key set but GunYu has no static keyspec.

The module samples are built into `tests/bisync/cmd/keyspec_verify/main.go`. The real Redis Stack validation result should look like:

```text
summary ... total=10 supported=10 ok=10 unsupported=0 mismatch=0 unresolved=0 error=0
```

## 4. Full RDB Sync

### 4.1 Why GunYu Does Not Parse Module Internals

The RDB payload of a Redis module object is defined by the module's own `rdb_save` / `rdb_load` implementation. The format may change across modules and versions.

If GunYu tried to parse RedisJSON, RediSearch, or RedisBloom internals itself, it would introduce three problems:

- It would need to track the private serialization formats of every module version.
- A parser failure could produce corrupt or incomplete restore results.
- Module upgrades would carry a high long-term compatibility cost.

So the implementation uses an opaque strategy: GunYu only guarantees that it reads and preserves the raw module value payload, then hands that payload to the target through Redis `RESTORE`.

### 4.2 What ModuleParser Does

In `pkg/rdb/rdb_object.go`, `ModuleParser` is responsible for:

- Reading the key.
- Reading the module id.
- Recording the module type name derived from the id.
- Fully consuming the module2 payload through `rdbLoadCheckModuleValue`.
- Preserving the raw bytes inside `BaseParser.buf`.

The key detail is `io.TeeReader`:

```go
r := NewRdbReader(io.TeeReader(lr, &mp.buf))
```

That lets the parser read the RDB while also copying the raw module value bytes into `mp.buf`. Later, `CreateValueDump()` rebuilds the payload format expected by Redis `RESTORE`:

```text
[rtype][raw module value bytes][rdb version][crc64]
```

### 4.3 RESTORE Is Mandatory

Module objects cannot be expanded into normal Redis commands, so `ExecCmd` no longer merely warns. It explicitly panics:

```go
panic(fmt.Errorf("module object requires RESTORE replay: id(%d), name(%s)", mp.id, mp.name))
```

Both regular RDB replay and bisync RDB replay protect this path:

- If the object is a module object, it must use `RESTORE`.
- If `replayRdbEnableRestore` is disabled, the dump exceeds `maxProtoBulkLen`, or the object is split, replay returns a clear error.
- There is no fallback to expanded replay, which avoids silent data loss.

### 4.4 Target-Side Requirements

The target Redis must satisfy all of the following:

- The corresponding modules are already loaded, for example `ReJSON` and `bf`.
- The module versions are compatible with the source RDB payload.
- `replay.replayRdbEnableRestore: true`.
- `maxProtoBulkLen` is large enough to hold the module dump.
- If the RDB contains RediSearch indexes or other `MODULE_AUX` global metadata, the default `replay.moduleAuxPolicy: fail` should stop replay and avoid checkpointing. Switch to `replay.moduleAuxPolicy: skip` only if that metadata is known to be rebuilt by incremental module commands or an external process.

Otherwise `RESTORE` fails and GunYu stops on error.

## 5. Handling In Bisync

The bisync RDB path converts each RDB entry into a replay unit.

Normal objects have two possible paths:

- If the object can be restored and is not split, generate a `RESTORE` command.
- If it cannot be restored or is too large and split, expand it into native commands such as `HSET`, `SADD`, or `XADD`.

Module objects have only one path:

```text
RDB module object -> RESTORE replay unit
```

If `RESTORE` cannot be used, `buildBisyncRdbReplayUnit` returns an error:

```text
rdb module object requires RESTORE replay for key ...
```

That guarantees bisync never treats a module object as empty and never tries to expand it into incorrect native commands.

## 6. Key Code Paths

| Function | File |
| --- | --- |
| Static keyspec for module commands | `pkg/redis/keyspec/keyspec.go` |
| Filtering and key projection tests | `pkg/filter/filter_test.go` |
| Real Redis keyspec verification for module commands | `tests/bisync/cmd/keyspec_verify/main.go` |
| RDB module object parsing | `pkg/rdb/rdb_object.go` |
| Module value payload consumption | `pkg/rdb/loader.go` |
| RESTORE enforcement in normal RDB replay | `pkg/rdbrestore/restore.go` |
| RESTORE enforcement in bisync RDB replay | `syncer/bisync_rdb.go` |
| Bisync RDB module unit tests | `syncer/bisync_rdb_test.go` |
| RDB module aux parsing tests | `pkg/rdb/loader_test.go` |

## 7. Validation

### 7.0 Regression Coverage

Redis Modules are currently covered by these main regression scripts:

```bash
bash ./tests/nonbisync/run_category11.sh
bash ./tests/bisync/run_category10.sh
```

Observed validation results:

- `tests/nonbisync/run_category11.sh` validates one-way incremental replay for `JSON.SET`, `JSON.DEL`, `JSON.MSET`, `BF.ADD`, `FT.CREATE`, and `FT.DROPINDEX`.
- `tests/bisync/run_category10.sh` validates Redis Stack `COMMAND GETKEYS` alignment, RedisJSON / RedisBloom RDB keyspace object restore, `moduleAuxPolicy=fail|skip` boundaries, and the fact that RediSearch index metadata is not restored.
- An additional probe validated incremental convergence for `TDIGEST.MERGE` and `TOPK.ADD`. `CMS.MERGE` is currently included only in keyspec alignment, not in the stable end-to-end release gate.

### 7.1 Module Command Keyspec Validation

Start Redis Stack:

```bash
docker run -d --name redis-stack-gunyu-test -p 6389:6379 redis/redis-stack-server:7.4.0-v8@sha256:798ab84d9f266936b034ab11c4d04a2b8e4b441884c5aa7d17ac951eefdf742a
```

Run the verifier:

```bash
GOCACHE=/private/tmp/redisgunyu-gocache \
go run ./tests/bisync/cmd/keyspec_verify \
  --addrs 127.0.0.1:6389 \
  --tags module \
  --fail-on-unsupported
```

Expected result:

```text
total=10 supported=10 ok=10 unsupported=0 mismatch=0 unresolved=0 error=0
```

### 7.2 RDB Keyspace Module Object Validation

Write RedisJSON / RedisBloom data on the source:

```bash
docker exec redis-stack-gunyu-src redis-cli JSON.SET doc:2 '$' '{"name":"bob","age":20}'
docker exec redis-stack-gunyu-src redis-cli BF.ADD bf:2 item-b
docker exec redis-stack-gunyu-src redis-cli SAVE
docker cp redis-stack-gunyu-src:/data/dump.rdb /private/tmp/redis-stack-gunyu-module-keyspace.rdb
```

Replay it with GunYu RDB load:

```yaml
action: load
rdbPath: /private/tmp/redis-stack-gunyu-module-keyspace.rdb
load:
  redis:
    addresses:
      - 127.0.0.1:6391
    type: standalone
  replay:
    keyExists: replace
    replayRdbEnableRestore: true
    moduleAuxPolicy: skip
    maxProtoBulkLen: 536870912
```

Run:

```bash
./redisGunYu -cmd=rdb -conf=/private/tmp/redisgunyu_module_rdb_load.yaml
```

Verify:

```bash
docker exec redis-stack-gunyu-dst redis-cli JSON.GET doc:2 '$'
docker exec redis-stack-gunyu-dst redis-cli BF.EXISTS bf:2 item-b
```

Expected:

```text
[{"name":"bob","age":20}]
1
```

If the RDB contains no `MODULE_AUX`, the default `moduleAuxPolicy: fail` can be used. If the RDB comes from Redis Stack and includes RediSearch indexes, `moduleAuxPolicy: fail` should stop replay as designed. To validate RedisJSON / RedisBloom keyspace object restore in that case, explicitly use `moduleAuxPolicy: skip` and make sure the skipped RediSearch indexes are rebuilt by an external process or by incremental `FT.CREATE`.

### 7.3 RediSearch RDB Boundary Validation

If the source creates a RediSearch index:

```bash
docker exec redis-stack-gunyu-src redis-cli FT.CREATE idx ON JSON PREFIX 1 doc: SCHEMA $.name AS name TEXT
```

The RDB will contain `MODULE_AUX`. GunYu currently logs messages like:

```text
unsupported module aux data skipped : module(ft_index0)
```

The target `FT._LIST` will not contain that index. This matches the current boundary: RediSearch incremental commands are supported, but full restore of RediSearch index metadata from RDB is not.

## 8. Current Limitations

- Validation today covers only the Redis Stack combination of `ReJSON`, `bf`, and `search`. Other third-party modules are not promised.
- Validated commands are limited to the command shapes listed in this document. Modules commonly add new commands or argument variants, and every new business command should be rechecked with `COMMAND GETKEYS` and end-to-end replay.
- `RdbTypeModule` type 1 is still unsupported. Modern Redis module RDB payloads primarily use `RdbTypeModule2`.
- `MODULE_AUX` is not restored. `moduleAuxPolicy: fail` stops replay; `skip` logs and continues.
- RediSearch indexes must be rebuilt through incremental `FT.CREATE` commands or an external deployment workflow. They cannot rely on RDB full restore.
- If the target does not load the corresponding modules, both module command replay and `RESTORE` fail.
- If source and target module versions are incompatible, `RESTORE` may fail. GunYu does not parse or transform private module payload formats.
- Even when a multi-key module command has a valid keyspec, its business semantics may still depend on other source objects already being present and compatible on the target. Merge-style commands are a common example.
- Query commands such as `FT.SEARCH` can have keyspec coverage for routing and verification, but they normally do not appear as AOF writes in incremental replay.
- In cluster and bisync mode, module commands whose key sets cannot be proven are treated as unsafe; the system should not rely on the target failing later as a recovery strategy.

## 9. Production Notes

Before enabling Redis Modules synchronization in production, check the following in order:

- Confirm that both source and target load the same or compatible modules with `MODULE LIST`.
- Confirm that all business module commands are present in the keyspec table, or extend validation through `keyspec_verify --samples-file`.
- Run real source-to-target convergence tests for your business commands instead of relying only on `COMMAND GETKEYS`.
- Keep `replayRdbEnableRestore: true` for full RDB replay and size `maxProtoBulkLen` high enough.
- If the RDB contains RediSearch indexes, keep the default `moduleAuxPolicy: fail` unless you have already decided how those indexes will be rebuilt.
- Do not assume RediSearch index metadata is restored after full sync; explicitly check `FT._LIST`.
- For RedisJSON / RedisBloom RDB restore, verify with module-native commands such as `TYPE`, `JSON.GET`, `BF.EXISTS`, `TDIGEST.INFO`, and `TOPK.LIST`.
- Re-run the Redis Modules regression suite whenever module versions, Redis Stack images, or business module command sets change.

## 10. Follow-up Recommendations

Recommended next steps:

- Put Redis Stack module validation into CI or a release gate.
- Keep extending `keyspec_verify` samples with real production commands.
- Add startup-time module capability prechecks such as `MODULE LIST` and `COMMAND INFO`.
- Add finer `MODULE_AUX` policies such as `ignore`, `warn`, or `error`.
- Expand the stable end-to-end validation matrix for more RedisBloom command combinations, especially merge / reserve / init flows.

If full RediSearch RDB restore is required later, there are two main paths:

- Official capability path: confirm whether RediSearch can restore indexes from forwarded AUX data on the target, then let GunYu preserve and replay AUX.
- Command-rebuild path: rebuild indexes by replaying `FT.CREATE` / `FT.ALTER` from external index definitions instead of relying on RDB AUX.

For now, the safest production model is: use RDB full restore for RedisJSON / RedisBloom keyspace data, and rebuild RediSearch indexes through deployment automation or incremental commands.
