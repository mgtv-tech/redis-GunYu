# Bisync 双向同步功能说明

- [Bisync 双向同步功能说明](#bisync-双向同步功能说明)
  - [1. 功能概述](#1-功能概述)
  - [2. 适用场景](#2-适用场景)
  - [3. 配置方式](#3-配置方式)
  - [4. 上线前注意事项](#4-上线前注意事项)
    - [4.1 业务 key 禁用前缀](#41-业务-key-禁用前缀)
    - [4.2 不同模式性能测试报告](#42-不同模式性能测试报告)
  - [5. 不支持或暂不建议使用的命令](#5-不支持或暂不建议使用的命令)
    - [5.1 暂不支持的模块命令](#51-暂不支持的模块命令)
    - [5.2 无法解析 key 的命令](#52-无法解析-key-的命令)
    - [5.3 cluster 下跨 slot 事务](#53-cluster-下跨-slot-事务)
    - [5.4 受限的部分投影](#54-受限的部分投影)
  - [6. 当前实现局限性](#6-当前实现局限性)
  - [7. 观测与排查](#7-观测与排查)
  - [8. 开发与实现说明](#8-开发与实现说明)
    - [8.1 工作机制](#81-工作机制)
    - [8.2 恢复与元数据](#82-恢复与元数据)
  - [9. 已覆盖和已有测试记录](#9-已覆盖和已有测试记录)
  - [10. 发布建议](#10-发布建议)


## 1. 功能概述

`bisync` 是 `redis-GunYu` 新增的双向同步能力，用于在两套 Redis 之间同时建立 A -> B 和 B -> A 两条同步链路。

它的核心目标是：

- 将两端业务写入同步到对端
- 抑制 GunYu 自己产生的镜像写入，避免 A -> B 的回放再次被 B -> A 回流
- 在 syncer 重启、Redis failover、目标 cluster 拓扑变化后尽量从可靠恢复点继续同步

当前实现支持：

- Redis standalone 双向同步
- Redis cluster 双向同步
- AOF 增量同步路径
- RDB 全量同步路径
- `sync` 回放模式
- `pipeline` 回放模式
- 目标端 `MOVED` / `ASK` 重定向下的 replay unit 级重试

当前实现采用真实 `MULTI/EXEC` 事务包装 replay unit，在事务内同时写入：

- 短生命周期 marker，用于反向链路识别镜像事务
- 业务命令
- bisync 恢复元数据

设计选型说明见 [bisync_scheme_selection.md](./bisync_scheme_selection.md)，实现细节见 [bisync_scheme1_impl.md](./bisync_scheme1_impl.md)，checkpoint key 说明见 [checkpoint_zh.md](./checkpoint_zh.md)。英文版文档见 [bisync_en.md](./bisync_en.md)、[bisync_scheme_selection_en.md](./bisync_scheme_selection_en.md)、[bisync_scheme1_impl_en.md](./bisync_scheme1_impl_en.md)。


## 2. 适用场景

适合使用 bisync 的场景：

- 两套 Redis 集群之间需要双向数据流动
- 业务能接受最终一致，而不是强一致多主复制
- 双端写入不会长期高频修改同一批 key
- 业务已经明确冲突语义，例如同 key 双边写入时接受复制顺序决定最终结果
- 需要通过 GunYu 的过滤规则控制同步范围

不适合直接使用 bisync 的场景：

- 需要数据库级强一致、多主冲突检测或 CRDT 语义
- 两端大量客户端同时写同一批 key，且不能接受覆盖、叠加或顺序差异
- Redis 模块命令是核心流量，但尚未完成 keyspec 与回归验证
- 业务依赖未被 GunYu 正确解析 key 的自定义命令

## 3. 配置方式

bisync 必须在两条同步链路上分别配置。也就是说，A -> B 和 B -> A 都要启动一个 syncer。

关键配置位于 `output.replay`：

```yaml
output:
  redis:
    # 按现有 GunYu 配置填写目标 Redis
  replay:
    resumeFromBreakPoint: true
    replayTransaction: true
    bisyncEnabled: true
    mode: sync
    keyExists: replace
```

字段说明：

| 配置 | 建议值 | 说明 |
| --- | --- | --- |
| `bisyncEnabled` | `true` | bisync 唯一显式开关，默认是 `false` |
| `replayTransaction` | `true` | 建议保持开启；它不是 bisync 开关，但普通回放仍会使用该配置 |
| `mode` | `sync` / `pipeline` | AOF 回放执行语义；非 bisync 下 `pipeline` 等价于旧 pipeline |
| `resumeFromBreakPoint` | `true` | 建议开启断点续传 |
| `keyExists` | `replace` / `ignore` / `error` | RDB 全量同步时目标 key 已存在的处理策略 |

`mode` 的含义：

- `sync`：逐个 replay unit 完成 `Dispatch + Receive` 后才发送下一个，恢复点来自每个 slot 的 `latest` checkpoint
- `pipeline`：发送和接收放到不同 goroutine，但按发送顺序确认并推进 checkpoint，通过 `commit journal + frontier` 恢复连续前缀

生产环境不要只验证一个模式就上线另一个模式。`sync` 与 `pipeline` 的恢复面不同，两种执行语义都应分别测试。
如果业务对双向同步数据一致性要求很高，建议优先使用 `sync`。当前 `pipeline` 模式在极端场景下仍可能出现数据一致性问题，不能按“严格一致”语义来理解。

## 4. 上线前注意事项

- 必须同时部署 A -> B 和 B -> A 两条链路，且两边都启用 `bisyncEnabled: true`
- 不要把 `replayTransaction` 当作 bisync 开关；bisync 只由 `bisyncEnabled` 决定
- 不要手动删除或改写 GunYu 控制面 key，尤其是 `redis-gunyu-bisync:*`、`redis-gunyu-checkpoint*` 和 `/redis-gunyu*`
- 不要把业务 key 命名到 GunYu 保留前缀下，具体见 [4.1 业务 key 禁用前缀](#41-业务-key-禁用前缀)
- cluster 模式下，业务事务应尽量使用 hash tag 保证单 slot
- 灰度初期建议按业务前缀或 slot 范围控制同步范围
- 上线前必须明确同 key 双边写入的业务语义
- 开启 `pipeline` 前必须单独验证恢复、failover 和长稳
- 对一致性敏感场景，避免直接使用 `pipeline` 作为默认模式；当前实现下，极端故障、重启恢复或 frontier 后存在 gap 时，仍可能出现最终数据不一致
- 如果修改过滤规则，应重新跑 category4 和基础收敛测试

### 4.1 业务 key 禁用前缀

以下前缀或 key 名属于 GunYu 控制面，业务不要写入、迁移、清理或复用这些命名空间：

| 前缀或 key 名 | 用途 | 对业务的要求 |
| --- | --- | --- |
| `redis-gunyu-bisync:` | bisync marker、slot-local latest checkpoint、commit journal、RDB record 等元数据 | 业务 key 不要使用该前缀 |
| `redis-gunyu-checkpoint` | GunYu checkpoint 根前缀；包含普通 checkpoint、`redis-gunyu-checkpoint-bisync:<id>` 和 `redis-gunyu-checkpoint-hash` | 业务 key 不要使用该前缀 |
| `/redis-gunyu` | GunYu 集群注册、选主等命名空间前缀 | 业务 key 不要使用该前缀 |

其中 bisync 常见控制 key 包括：

- `redis-gunyu-checkpoint-bisync:<id>`
- `redis-gunyu-checkpoint-bisync:<id>:frontier`
- `redis-gunyu-bisync:<checkpointName>:marker:{slotTag}`
- `redis-gunyu-bisync:<checkpointName>:latest:{slotTag}`
- `redis-gunyu-bisync:<checkpointName>:commit:{slotTag}:<unitSeq>`
- `redis-gunyu-bisync:<checkpointName>:index:{slotTag}`
- `redis-gunyu-bisync:<checkpointName>:rdb:{slotTag}:<unitSeq>`

如果已有业务 key 命中这些前缀，应先改名或通过过滤规则排除，再启用 bisync。

### 4.2 不同模式性能测试报告


测试报告结果如下：
- [bisync_perf_report.md](./bisync_perf_report.md)



## 5. 不支持或暂不建议使用的命令

### 5.1 暂不支持的模块命令

当前 RedisJSON / RedisBloom 等模块命令尚未形成稳定发布门禁，以下命令暂按不支持管理：

| 模块 | 命令 |
| --- | --- |
| RedisJSON | `JSON.SET`、`JSON.DEL`、`JSON.MSET` |
| RedisBloom | `BF.ADD`、`CMS.MERGE`、`TDIGEST.MERGE`、`TOPK.ADD` |

原因是这些命令需要同时满足：

- 目标 Redis 已加载对应模块
- `COMMAND GETKEYS` 或静态 keyspec 能给出可证明的 key 集合
- strict routing 能判断 cluster 下是否单 slot
- 已纳入 `tests/bisync/run_category4.sh` 或扩展样本的稳定回归

在完成这些验证前，生产流量中应避免让这类命令进入 bisync 链路。

### 5.2 无法解析 key 的命令

bisync cluster 路径依赖真实 key 集合。如果静态 keyspec 未覆盖，且目标 Redis 的 `COMMAND GETKEYS` 也无法解析，syncer 会 fail-stop，而不是 best-effort 回放。

这类命令需要先补充 keyspec 或通过 `keyspec_verify` 验证后再放开。

### 5.3 cluster 下跨 slot 事务

cluster 模式下，单个 replay unit 必须绑定到一个 slot。跨 slot 的源端事务不能被包装成一个目标端真实事务。

当前策略是保守失败，避免把 correctness 建立在部分成功或伪事务上。

### 5.4 受限的部分投影

过滤规则导致事务内只有部分 key 命中时，当前只对少数命令开放安全投影，例如：

- `MSET`
- `DEL`
- `UNLINK`

其他 multi-key 命令会按保守策略处理，不能证明安全时会失败或拒绝投影。

## 6. 当前实现局限性

1. bisync 不提供业务冲突解决

   如果两边同时写同一个 key，GunYu 不会判断谁是“正确值”。最终结果由 Redis 命令语义、复制顺序和恢复过程共同决定。`INCR`、`LPUSH`、`XADD` 等非幂等命令尤其需要业务侧确认语义。

2. `pipeline` 恢复不是“精确跳过所有已提交 unit”

   当前 `pipeline` 模式通过 `frontier + commit journal` 重建连续已提交前缀。如果崩溃时 frontier 后存在 gap，gap 后面即使有 durable commit，也可能在重启后随 source stream 再次回放。当前语义是从最后连续已提交点恢复，而不是逐个精确跳过所有已提交 unit。

   这意味着在极端故障、异常退出或恢复窗口内，`pipeline` 模式不能承诺严格数据一致性；如果业务不能接受这类风险，应使用 `sync` 模式，或在上线前准备额外的对账与回补方案。

3. RDB 路径不提供 key 级 authoritative 恢复点

   RDB bisync 路径用于回环抑制和全量回放，但 authoritative 恢复仍以整份 RDB 完成后的 checkpoint barrier 为准。

4. cluster 全局对象仍有边界

   `FUNCTION RESTORE` 等非 key-based 全局 opcode 不能完全纳入 slot-local 真实事务模型，仍需要按当前实现的特例和回归结果管理。

5. 拓扑剧烈抖动时以 fail-stop 为主

   当前已支持 `MOVED` / `ASK` 下 replay unit 级重试，但如果目标 cluster 持续重定向或长时间不稳定，syncer 会中断，而不是降级成弱一致模式。

6. Redis 版本兼容性需要按生产版本验证

   尤其是 RDB 格式、keyspec、模块命令和 `COMMAND GETKEYS` 行为。发布前应使用生产同版本 Redis 跑 bisync 测试。

## 7. 观测与排查

可通过 HTTP API 查看 syncer 状态：

```bash
curl http://127.0.0.1:<http-port>/syncer/status
```

bisync 相关指标包括：

| 指标 | 含义 |
| --- | --- |
| `bisync_unit_build` | replay unit 构建计数 |
| `bisync_txn_commit` | bisync 事务提交计数 |
| `bisync_single_slot_fail` | 单 slot 校验失败计数 |
| `bisync_txn_suppress` | mirrored transaction 抑制计数 |
| `bisync_frontier_seq` | `pipeline` frontier 序号 |
| `bisync_frontier_offset` | `pipeline` frontier offset |
| `bisync_frontier_rebuild_seconds` | frontier 重建耗时 |
| `bisync_commit_backlog` | `pipeline` commit backlog |
| `bisync_commit_gc` | commit journal 清理计数 |

重点关注：

- `bisync_txn_commit` 是否持续增长
- `bisync_txn_suppress` 是否能看到镜像事务被抑制
- `bisync_single_slot_fail` 是否增长
- 高负载下 `bisync_commit_backlog` 是否长期堆积
- syncer 日志中是否出现 repeated `MOVED` / `ASK`、frontier rebuild、journal gap、strict routing 失败

## 8. 开发与实现说明

### 8.1 工作机制

bisync 会把源端复制流切成 replay unit：

- 普通命令：一条命令一个 replay unit
- 源端事务：整个 `MULTI ... EXEC` 一个 replay unit
- RDB entry：按 key 或 split key 生成 replay unit

在目标 Redis 上，每个 replay unit 会以真实事务提交：

```redis
MULTI
SET redis-gunyu-bisync:<checkpointName>:marker:{slotTag} <marker> PX <ttl>
... business commands ...
HSET/ZADD ... bisync metadata ...
EXEC
```

反向链路解析 AOF 时，如果发现事务内存在合法 marker，就把整笔 mirrored transaction 抑制掉，不再回放给原始来源端。

cluster 模式下，每个 replay unit 必须能证明所有业务 key 属于同一个 slot。standalone 模式下使用 synthetic slot `0`，允许事务内 key 分布在任意 hash slot。

### 8.2 恢复与元数据

bisync 仍然以 source `runId + offset` 作为复制流恢复轴，但恢复元数据会放到稳定的 `checkpointName` namespace 下。

常见 key 包括：

| key | 用途 |
| --- | --- |
| `redis-gunyu-checkpoint-bisync:<id>` | bisync namespace root |
| `redis-gunyu-checkpoint-bisync:<id>:frontier` | `pipeline` 模式的全局连续恢复点 |
| `redis-gunyu-bisync:<checkpointName>:marker:{slotTag}` | 镜像事务抑制 marker |
| `redis-gunyu-bisync:<checkpointName>:latest:{slotTag}` | `sync` 模式 slot-local 恢复点 |
| `redis-gunyu-bisync:<checkpointName>:commit:{slotTag}:<unitSeq>` | `pipeline` 模式提交 journal |
| `redis-gunyu-bisync:<checkpointName>:index:{slotTag}` | `pipeline` 模式 journal 索引 |
| `redis-gunyu-bisync:<checkpointName>:rdb:{slotTag}:<unitSeq>` | RDB 回放路径记录 |
| `redis-gunyu-checkpoint-hash` | source runId 到 checkpoint namespace 的映射 |

这些 key 属于 GunYu 控制面，不应由业务读写、迁移或清理。

## 9. 已覆盖和已有测试记录

当前仓库已经提供 `tests/bisync` 测试集。

单元测试覆盖重点：

- replay unit 构建
- mirrored transaction 抑制
- mirrored RDB transaction 抑制
- RDB `keyExists=replace/ignore/error`
- split key skip 处理
- strict routing 和 `COMMAND GETKEYS` fallback
- filtered transaction projection
- frontier rebuild
- checkpoint namespace 迁移
- cluster 恢复扫描全 slot
- `CanTransaction=false` 时 bisync 仍保持启用
- cluster 事务 batcher 对 `MOVED` / `ASK` 的重试

集成测试脚本：

| 脚本 | 覆盖内容 |
| --- | --- |
| `tests/bisync/run_category1.sh` | 基础双向收敛 |
| `tests/bisync/run_category2.sh` | `sync` / `pipeline` 重启、断点续传、metadata 形态 |
| `tests/bisync/run_category3.sh` | RDB 特殊路径、纯全量同步、full-sync barrier |
| `tests/bisync/run_category4.sh` | keyspec、过滤、strict routing、真实 Redis `COMMAND GETKEYS` 校验 |
| `tests/bisync/run_category5.sh` | source failover、target failover、syncer restart |
| `tests/bisync/run_category6.sh` | 外部 cluster 多数据结构集成测试 |
| `tests/bisync/run_category7.sh` | 外部 cluster 持续写入 soak 测试 |
| `tests/bisync/run_category8.sh` | 非 bisync 单向链路回归，确认不会产生 bisync 元数据 |
| `tests/bisync/run_category9.sh` | 发布前耐久长稳，支持 `2h`、`4h`、`6h` 分档 |

仓库中已有一份长稳记录：

- `tests/bisync/reports/` 下已有一份 `sync` 模式的长稳记录可供参考
- 使用 Redis 7.0.11、`sync` 模式、4 worker、目标约 `10000` combined logical commands/s
- 实际采样运行约 `87.48m`
- 已完成 syncer API restart、左右 Redis failover、syncer offline/resume、final syncer API restart 等计划故障注入
- 日志检查未发现 panic、fatal、connection reset by peer
- 该次运行是人工停止，没有进入脚本最终 compare/report 阶段，因此不能作为完整 `2h` 发布门禁通过记录

发布前建议至少完成：

```bash
go test ./...
bash ./tests/bisync/run_category1.sh
bash ./tests/bisync/run_category2.sh
bash ./tests/bisync/run_category3.sh
bash ./tests/bisync/run_category4.sh
bash ./tests/bisync/run_category5.sh
bash ./tests/bisync/run_category8.sh
SOAK_TIER=2h SCENARIOS=sync,pipeline KEEP_TMP=1 bash ./tests/bisync/run_category9.sh
```

`category9` 的 `2h` 通过并审阅报告后，再推进 `4h` 和 `6h`。

## 10. 发布建议

推荐发布步骤：

1. 使用生产同版本 Redis 跑完单测和 `category1` 到 `category5`、`category8`
2. 使用 `category9` 分档完成 `sync` 和 `pipeline` 长稳
3. 按业务前缀或 slot 范围灰度，控制初始写入规模
4. 观察状态接口、bisync 指标、Redis 资源和业务比对结果
5. 灰度期间保留快速关闭 `bisyncEnabled` 或停止反向链路的回滚方案

生产通过标准：

- 双端业务 key 最终一致
- `sync` 和 `pipeline` 的目标模式均已验证
- failover、syncer restart、离线恢复后仍能收敛
- 不存在异常残留的 commit journal 堆积
- 未出现持续增长的 goroutine、RSS、storer 目录或 Redis memory 异常
- 业务命令不包含未验证的不支持命令
