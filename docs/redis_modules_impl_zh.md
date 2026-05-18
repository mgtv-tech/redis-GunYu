# Redis Modules 支持实现说明

- [1. 背景与目标](#1-背景与目标)
- [2. 支持范围](#2-支持范围)
- [3. 模块命令增量同步](#3-模块命令增量同步)
- [4. RDB 全量同步](#4-rdb-全量同步)
- [5. Bisync 场景下的处理](#5-bisync-场景下的处理)
- [6. 关键代码路径](#6-关键代码路径)
- [7. 验证方法](#7-验证方法)
- [8. 当前限制](#8-当前限制)
- [9. 生产使用注意事项](#9-生产使用注意事项)
- [10. 后续扩展建议](#10-后续扩展建议)

## 1. 背景与目标

RedisJSON、RediSearch、RedisBloom 等 Redis Modules 会带来两类同步问题：

- AOF / 命令增量同步时，需要知道命令访问哪些 key，才能在 cluster 和 bisync 场景下正确路由。
- RDB / 全量同步时，模块自定义数据类型会以 Redis module object 的二进制格式写入 RDB，GunYu 不能按普通 string/hash/list 等内置类型展开。

本次优化的目标是：

- 为已验证模块命令补充静态 keyspec，保证增量链路可路由、可过滤、可校验。
- 对 RDB keyspace module object 采用 opaque `RESTORE`，不解析模块内部格式。
- 明确 RediSearch index 这类 `MODULE_AUX` 全局模块元数据的限制，避免静默丢索引。

## 2. 支持范围

Redis Modules 当前是**部分支持**，不能按“所有模块命令、所有模块数据类型都完整支持”理解。本文把支持程度分成四类：

| 支持等级 | 含义 | 可用于生产的前提 |
| --- | --- | --- |
| 静态 keyspec 已覆盖 | GunYu 能解析命令涉及的 key，可用于路由、过滤和 bisync slot 绑定 | 仍需要目标 Redis 加载对应模块，并用业务真实命令做端到端验证 |
| 实时增量端到端已验证 | 已在 Redis Stack 环境中实际写源端并确认目标端收敛 | 仅覆盖表中列出的命令形态和 Redis Stack 模块组合 |
| RDB keyspace object 已验证 | 模块 keyspace 数据可通过 opaque `RESTORE` 恢复 | 目标端必须加载兼容模块，且 `replayRdbEnableRestore: true` |
| 不支持 / 不承诺支持 | 缺少 keyspec、缺少端到端验证、依赖 `MODULE_AUX`、目标端无模块或模块格式不兼容 | 需要新增实现、验证样本或外部重建流程 |

当前已验证的模块命令支持程度如下：

| 模块 | 命令 | keyspec / 路由 | 实时增量端到端 | 说明 |
| --- | --- | --- | --- | --- |
| RedisJSON | `JSON.SET`、`JSON.DEL`、`JSON.MSET` | 已支持 | 已通过 | `tests/nonbisync/run_category11.sh` 已验证源端写入后目标端 JSON 值收敛 |
| RedisBloom | `BF.ADD` | 已支持 | 已通过 | `tests/nonbisync/run_category11.sh` 已验证 Bloom membership 收敛 |
| RedisBloom | `TDIGEST.MERGE` | 已支持 | 已通过补充探针 | keyspec 与 Redis Stack 一致，只把目标 digest 作为 key |
| RedisBloom | `TOPK.ADD` | 已支持 | 已通过补充探针 | 依赖目标端已有 `TOPK.RESERVE` 创建的 topk key |
| RedisBloom | `CMS.MERGE` | 已支持 | 未作为稳定端到端结果确认 | keyspec 与 Redis Stack 一致，只把目标 sketch 作为 key；本次补充探针仅确认 `CMS.INITBYDIM` / `CMS.INCRBY` 前置 key 可同步，`CMS.MERGE` 结果未纳入稳定门禁 |
| RediSearch | `FT.CREATE`、`FT.DROPINDEX` | 已支持 | 已通过 | `tests/nonbisync/run_category11.sh` 已验证 index 创建和删除可同步 |
| RediSearch | `FT.SEARCH` | 已支持 | 不适用 | 这是查询命令，正常不会作为 AOF 写命令进入实时同步；支持 keyspec 主要用于路由/校验工具 |

代码中还为更多 RedisJSON / RedisBloom 命令补充了静态 keyspec，例如 `JSON.ARRAPPEND`、`JSON.NUMINCRBY`、`BF.MADD`、`CMS.INCRBY`、`TDIGEST.ADD`、`TOPK.RESERVE` 等。它们可以被 GunYu 解析 key，但**没有全部纳入本文的稳定端到端验证矩阵**。生产使用时应把业务实际命令追加到 `keyspec_verify --samples-file`，并跑源端到目标端的收敛验证。

当前已验证的 RDB module object 恢复如下：

| 模块数据 | RDB 恢复方式 | 状态 | 说明 |
| --- | --- | --- | --- |
| RedisJSON keyspace object | `RESTORE` 原始 dump | 已通过 Redis Stack 验证 | 不解析 JSON 内部格式，由目标端 ReJSON 模块加载 |
| RedisBloom keyspace object | `RESTORE` 原始 dump | 已通过 Redis Stack 验证 | 不解析 Bloom/CMS/TDigest/TopK 内部格式，由目标端 bf 模块加载 |
| RediSearch index metadata | `MODULE_AUX` | 不支持全量恢复 | index 是全局模块元数据，不是普通 keyspace object，当前只能 fail 或 skip |
| 其他 Redis Modules | 未定义 | 不承诺支持 | 需要确认命令 keyspec、RDB payload、目标模块兼容性和端到端回放结果 |

### 2.1 明确不支持或不承诺支持的范围

- 未列入静态 keyspec 或未通过 `keyspec_verify` 的模块命令，不承诺支持。原因是 GunYu 无法可靠知道命令访问哪些 key，cluster / bisync / 过滤场景下可能路由错误。
- 只在代码中有静态 keyspec、但没有业务端到端验证的模块命令，只能视为“key 解析可用”，不能直接等同于“业务语义已验证”。
- RediSearch index 的 RDB 全量恢复不支持。原因是 index metadata 写在 `MODULE_AUX` 全局元数据中，不是 keyspace module object，不能通过单 key `RESTORE` 恢复。
- 目标 Redis 未加载对应模块时不支持。原因是 GunYu 只能回放命令或 `RESTORE` dump，不能替目标端实现模块命令。
- 源端和目标端模块版本不兼容时不支持。原因是 module object 的 RDB payload 是模块私有格式，GunYu 不解析也不转换。
- `RdbTypeModule` type 1 不支持。现代 Redis module RDB 主要使用 `RdbTypeModule2`，当前实现遇到 type 1 会停止。

## 3. 模块命令增量同步

### 3.1 为什么需要 keyspec

GunYu 在 cluster 与 bisync 场景下必须知道命令的真实 key 集合，原因包括：

- 选择目标 Redis cluster 节点。
- 判断 multi-key 命令是否跨 slot。
- 执行 key / slot 过滤。
- 在 bisync replay unit 中绑定 slot-local marker 和恢复元数据。

Redis 内置命令可以通过项目里的静态 keyspec 覆盖。模块命令如果没有静态 keyspec，运行时只能依赖目标 Redis 的 `COMMAND GETKEYS`。这会带来两个问题：

- 目标 Redis 未加载模块时，`COMMAND GETKEYS` 会返回 unknown command。
- 部分路径需要提前做严格路由，不能等到命令发送失败后再处理。

因此本次把已验证模块命令纳入 `pkg/redis/keyspec/keyspec.go`。

需要特别注意：**keyspec 支持不等于完整业务语义支持**。keyspec 只回答“这个命令访问哪些 key”，用于路由、过滤和校验；真正的实时同步是否可用，还取决于：

- 源端是否会把该模块命令或等价写命令写入复制流。
- 目标端是否加载了对应模块。
- 目标端模块版本是否能接受该命令参数和现有数据结构。
- 多 key 模块命令的源 key / 目标 key 是否已经在目标端具备一致状态。
- 该命令是否已经经过端到端收敛验证。

因此文档中把“keyspec 已覆盖”和“实时增量端到端已验证”分开标注。新增模块命令时，至少需要同时补充静态 keyspec、`keyspec_verify` 样本和真实 Redis Stack 回放验证。

### 3.2 静态 keyspec 规则

RedisJSON 和大多数 RedisBloom 命令以第一个参数为 key，例如：

```text
JSON.SET doc{t} $ {"a":1}
BF.ADD bf{t} item
TOPK.ADD topk{t} item
```

这些命令使用 `genericKeyPos`。

`JSON.MSET` 每三个参数一组，key 位于第 1、4、7... 个参数：

```text
JSON.MSET doc1{t} $ {"a":1} doc2{t} $ {"b":2}
```

因此它使用 `{1, -1, 3}`。

`CMS.MERGE` 和 `TDIGEST.MERGE` 需要特别处理。真实 Redis Stack 的 `COMMAND GETKEYS` 只返回目标 key：

```text
CMS.MERGE dst{t} 2 src1{t} src2{t}
TDIGEST.MERGE dst{t} 2 src1{t} src2{t}
```

返回：

```text
dst{t}
```

所以 GunYu 也只把第一个参数作为 key，不能把源 sketch / digest 参数误判为 key，否则会产生 false cross-slot。这个规则与 Redis Stack 的 `COMMAND GETKEYS` 保持一致，但也意味着这类命令的源对象一致性必须由前置同步保证；keyspec 本身不会校验源对象是否已经存在或内容是否一致。

RediSearch 当前覆盖索引名作为 key：

```text
FT.CREATE idx{t} ...
FT.SEARCH idx{t} ...
FT.DROPINDEX idx{t}
```

这让 cluster 路由可以以 index name 作为命令 key。

### 3.3 验证器

`tests/bisync/cmd/keyspec_verify` 会把 GunYu 静态 keyspec 和 Redis `COMMAND GETKEYS` 做对比。

核心判断：

- `ok`：静态 key 集和 Redis 返回 key 集一致。
- `unsupported`：目标 Redis 不支持该模块命令，通常是未加载模块。
- `mismatch`：静态 key 集和 Redis 返回不一致。
- `unresolved`：Redis 能解析，但 GunYu 静态 keyspec 缺失。

模块样本已内置到 `tests/bisync/cmd/keyspec_verify/main.go`。真实 Redis Stack 验证结果应满足：

```text
summary ... total=10 supported=10 ok=10 unsupported=0 mismatch=0 unresolved=0 error=0
```

## 4. RDB 全量同步

### 4.1 为什么不解析模块内部格式

Redis module object 的 RDB payload 由模块自己的 `rdb_save` / `rdb_load` 实现决定。不同模块、不同版本之间格式可能变化。

如果 GunYu 自己解析 RedisJSON、RediSearch、RedisBloom 的内部格式，会引入三个问题：

- 需要追踪每个模块版本的私有序列化格式。
- 解析失败容易造成数据损坏或不完整恢复。
- 模块升级后兼容性成本很高。

因此实现采用 opaque 策略：GunYu 只保证完整读取并保留 module value 原始 payload，恢复时通过 Redis `RESTORE` 交给目标端模块处理。

### 4.2 ModuleParser 的职责

`pkg/rdb/rdb_object.go` 中的 `ModuleParser` 负责：

- 读取 key。
- 读取 module id。
- 根据 module id 记录模块类型名。
- 使用 `rdbLoadCheckModuleValue` 完整消费 module2 payload。
- 把读取到的原始 bytes 保存在 `BaseParser.buf` 中。

关键点是 `io.TeeReader`：

```go
r := NewRdbReader(io.TeeReader(lr, &mp.buf))
```

这让 parser 在读取 RDB 的同时，把 module value 原始 bytes 写入 `mp.buf`。之后 `CreateValueDump()` 会重新组装 Redis `RESTORE` 需要的 payload：

```text
[rtype][raw module value bytes][rdb version][crc64]
```

### 4.3 强制使用 RESTORE

module object 不能展开成普通 Redis 命令，因此 `ExecCmd` 不再只是 warn，而是显式 panic：

```go
panic(fmt.Errorf("module object requires RESTORE replay: id(%d), name(%s)", mp.id, mp.name))
```

普通 RDB replay 和 bisync RDB replay 都做了保护：

- 如果对象是 module object，则必须走 `RESTORE`。
- 如果 `replayRdbEnableRestore` 关闭、dump 超过 `maxProtoBulkLen`、对象被拆分，直接返回明确错误。
- 不允许 fallback 到 expanded replay，避免静默丢数据。

### 4.4 目标端要求

目标 Redis 必须满足：

- 已加载对应模块，例如 `ReJSON`、`bf`。
- 模块版本与源端 RDB payload 兼容。
- `replay.replayRdbEnableRestore: true`。
- `maxProtoBulkLen` 足够容纳 module dump。
- 如果 RDB 中包含 RediSearch index 等 `MODULE_AUX` 全局模块元数据，默认 `replay.moduleAuxPolicy: fail` 会停止回放并避免写 checkpoint。只有确认这些元数据会通过增量模块命令或外部流程重建时，才设置 `replay.moduleAuxPolicy: skip`。

否则 `RESTORE` 会失败，GunYu 会按错误停止。

## 5. Bisync 场景下的处理

Bisync RDB 路径会把 RDB entry 转成 replay unit。

普通对象有两种路径：

- 可恢复且未拆分时，生成一个 `RESTORE` 命令。
- 不能恢复或超大拆分时，展开成原生命令，例如 `HSET`、`SADD`、`XADD`。

module object 只有一种路径：

```text
RDB module object -> RESTORE replay unit
```

如果不能走 `RESTORE`，`buildBisyncRdbReplayUnit` 会返回错误：

```text
rdb module object requires RESTORE replay for key ...
```

这样可以保证双向同步不会把 module object 当作空对象跳过，也不会试图展开成错误命令。

## 6. 关键代码路径

| 功能 | 文件 |
| --- | --- |
| 模块命令静态 keyspec | `pkg/redis/keyspec/keyspec.go` |
| 过滤与 key 投影测试 | `pkg/filter/filter_test.go` |
| 模块命令真实 Redis 对比验证 | `tests/bisync/cmd/keyspec_verify/main.go` |
| RDB module object 读取 | `pkg/rdb/rdb_object.go` |
| module value payload 消费 | `pkg/rdb/loader.go` |
| 普通 RDB replay 的 module RESTORE 保护 | `pkg/rdbrestore/restore.go` |
| bisync RDB replay 的 module RESTORE 保护 | `syncer/bisync_rdb.go` |
| bisync RDB module 单测 | `syncer/bisync_rdb_test.go` |
| RDB module aux 解析测试 | `pkg/rdb/loader_test.go` |

## 7. 验证方法

### 7.0 回归脚本覆盖

当前 Redis Modules 相关回归主要由以下脚本覆盖：

```bash
bash ./tests/nonbisync/run_category11.sh
bash ./tests/bisync/run_category10.sh
```

实际验证结论：

- `tests/nonbisync/run_category11.sh` 验证单向实时增量同步，覆盖 `JSON.SET`、`JSON.DEL`、`JSON.MSET`、`BF.ADD`、`FT.CREATE`、`FT.DROPINDEX`。
- `tests/bisync/run_category10.sh` 验证 Redis Stack `COMMAND GETKEYS` 对比、RedisJSON / RedisBloom RDB keyspace object 恢复、`moduleAuxPolicy=fail|skip` 边界，以及 RediSearch index metadata 不恢复。
- 补充探针验证过 `TDIGEST.MERGE`、`TOPK.ADD` 的实时目标端收敛；`CMS.MERGE` 当前只纳入 keyspec 对齐，未作为稳定端到端结果门禁。

### 7.1 模块命令 keyspec 验证

启动 Redis Stack：

```bash
docker run -d --name redis-stack-gunyu-test -p 6389:6379 redis/redis-stack-server:latest
```

运行验证器：

```bash
GOCACHE=/private/tmp/redisgunyu-gocache \
go run ./tests/bisync/cmd/keyspec_verify \
  --addrs 127.0.0.1:6389 \
  --tags module \
  --fail-on-unsupported
```

期望结果：

```text
total=10 supported=10 ok=10 unsupported=0 mismatch=0 unresolved=0 error=0
```

### 7.2 RDB keyspace module object 验证

源端写入 RedisJSON / RedisBloom：

```bash
docker exec redis-stack-gunyu-src redis-cli JSON.SET doc:2 '$' '{"name":"bob","age":20}'
docker exec redis-stack-gunyu-src redis-cli BF.ADD bf:2 item-b
docker exec redis-stack-gunyu-src redis-cli SAVE
docker cp redis-stack-gunyu-src:/data/dump.rdb /private/tmp/redis-stack-gunyu-module-keyspace.rdb
```

使用 GunYu RDB load 恢复：

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

执行：

```bash
./redisGunYu -cmd=rdb -conf=/private/tmp/redisgunyu_module_rdb_load.yaml
```

验证：

```bash
docker exec redis-stack-gunyu-dst redis-cli JSON.GET doc:2 '$'
docker exec redis-stack-gunyu-dst redis-cli BF.EXISTS bf:2 item-b
```

期望：

```text
[{"name":"bob","age":20}]
1
```

如果 RDB 中没有 `MODULE_AUX`，可以使用默认的 `moduleAuxPolicy: fail`。如果 RDB 来自 Redis Stack 且包含 RediSearch index，`moduleAuxPolicy: fail` 会按预期停止回放；需要验证 RedisJSON / RedisBloom keyspace object 恢复时，应显式使用 `moduleAuxPolicy: skip`，并确认被跳过的 RediSearch index 会由外部流程或增量 `FT.CREATE` 重建。

### 7.3 RediSearch RDB 边界验证

如果源端创建 RediSearch index：

```bash
docker exec redis-stack-gunyu-src redis-cli FT.CREATE idx ON JSON PREFIX 1 doc: SCHEMA $.name AS name TEXT
```

RDB 中会包含 `MODULE_AUX`。当前 GunYu 会记录类似日志：

```text
unsupported module aux data skipped : module(ft_index0)
```

目标端 `FT._LIST` 不会出现该 index。这个行为符合当前边界：RediSearch 命令增量同步支持，RDB index metadata 全量恢复暂不支持。

## 8. 当前限制

- 仅验证了 Redis Stack 中的 `ReJSON`、`bf`、`search` 模块组合；其他第三方模块不承诺支持。
- 已验证命令只覆盖本文表格中的命令形态。模块通常会新增命令或参数变体，新增业务命令必须重新跑 `COMMAND GETKEYS` 对比和端到端回放。
- `RdbTypeModule` type 1 仍不支持；现代 Redis module RDB 主要使用 `RdbTypeModule2`。
- `MODULE_AUX` 不恢复；`moduleAuxPolicy: fail` 会停止回放，`skip` 会跳过并记录日志。
- RediSearch index 需要依赖 `FT.CREATE` 等增量命令或外部部署流程重建，不能依赖 RDB 全量恢复。
- 如果目标端未加载对应模块，模块命令和 `RESTORE` 都会失败。
- 如果目标端模块版本不兼容源端 RDB payload，`RESTORE` 可能失败；GunYu 不解析、不转换模块私有 payload。
- 多 key 模块命令即使 keyspec 可解析，也需要确认业务语义是否依赖其他源对象。例如 merge 类命令的源对象必须已经同步到目标端并保持兼容。
- 查询类命令例如 `FT.SEARCH` 可做 keyspec 校验，但正常不会作为 AOF 写命令进入实时同步。
- cluster / bisync 场景下，模块命令若无法证明 key 集合，会被视为不安全命令；不能依赖目标端执行失败后再补救。

## 9. 生产使用注意事项

上线 Redis Modules 同步前，建议按以下顺序确认：

- 确认源端和目标端都加载相同或兼容的模块：`MODULE LIST`。
- 确认业务实际使用的模块命令都在 keyspec 表中，或通过 `keyspec_verify --samples-file` 扩展验证。
- 对业务命令跑真实源端到目标端收敛测试，不只看 `COMMAND GETKEYS`。
- RDB 全量阶段保持 `replayRdbEnableRestore: true`，并把 `maxProtoBulkLen` 配到足够大。
- RDB 包含 RediSearch index 时，默认使用 `moduleAuxPolicy: fail` 保护数据完整性；只有确认 index 会由外部流程或增量命令重建时，才使用 `skip`。
- 不要把 RediSearch index metadata 视为已全量恢复；全量后应显式检查 `FT._LIST`。
- 对 RedisJSON / RedisBloom 的 RDB 恢复，恢复后应检查 `TYPE`、`JSON.GET`、`BF.EXISTS`、`TDIGEST.INFO`、`TOPK.LIST` 等模块原生命令。
- 模块版本升级、Redis Stack 镜像升级或业务新增模块命令后，都应重新跑 Redis Modules 回归脚本。

## 10. 后续扩展建议

优先建议：

- 把 Redis Stack 模块验证纳入 CI 或发布门禁。
- 按生产实际命令继续扩展 `keyspec_verify` 样本。
- 在启动阶段增加模块能力预检，例如检查 `MODULE LIST` 和 `COMMAND INFO`。
- 对 `MODULE_AUX` 增加更细的策略配置，例如 `ignore`、`warn`、`error`。
- 将更多 RedisBloom 命令纳入稳定端到端验证矩阵，特别是 merge / reserve / init 类命令组合。

如果后续必须支持 RediSearch RDB 全量恢复，有两条路线：

- 官方能力路线：确认 RediSearch 是否能通过目标端模块加载 AUX 并恢复 index，再让 GunYu 保存并转发 AUX。
- 命令重建路线：从业务配置或外部索引定义中重放 `FT.CREATE` / `FT.ALTER`，不依赖 RDB AUX。

当前更稳妥的生产建议是：RDB 全量恢复负责 RedisJSON / RedisBloom keyspace 数据，RediSearch index 由部署流程或增量命令重建。
