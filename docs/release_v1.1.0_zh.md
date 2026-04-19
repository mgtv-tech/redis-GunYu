# redis-GunYu v1.1.0 发布说明

## 1. 版本概述

`v1.1.0` 引入 `bisync` 双向同步能力，用于在两套 Redis 之间同时建立 `A -> B` 和 `B -> A` 两条同步链路。

本版本的目标不是把 `redis-GunYu` 升级成强一致多主数据库，而是在可接受最终一致的业务场景下，提供一套可恢复、可观测、可灰度发布的双向同步实现。

## 2. 主要发布内容

- 新增 `bisync` 双向同步能力，支持 Redis standalone 和 Redis cluster
- 支持 AOF 增量同步路径和 RDB 全量同步路径
- 通过真实 `MULTI/EXEC` 事务提交 replay unit，同时写入业务命令、镜像抑制 marker 和恢复元数据
- 支持 syncer 重启、Redis failover、目标 cluster 拓扑变化后的恢复
- 新增 `output.replay.mode` 配置，统一回放执行语义
- 保留旧 `enableAofPipeline` 配置的兼容加载，但新配置建议统一迁移到 `mode`
- 增加 bisync 设计说明、实现说明、测试设计、性能报告和运维注意事项文档

## 3. 配置与升级说明

### 3.1 新增或重点配置

双向同步相关配置位于 `output.replay`：

```yaml
output:
  replay:
    resumeFromBreakPoint: true
    replayTransaction: true
    bisyncEnabled: true
    mode: sync
    keyExists: replace
```

关键说明：

- `bisyncEnabled` 是 bisync 的唯一显式开关
- `replayTransaction` 建议保持开启，但它不是 bisync 开关
- `mode` 用于统一描述 AOF 回放执行语义；本版本对外发布口径只包含 `sync` 和 `pipeline`
- 对一致性敏感的生产场景，推荐优先使用 `mode: sync`

### 3.2 兼容性说明

- 旧 `enableAofPipeline` 配置仍可兼容加载
- 新配置建议统一改为 `output.replay.mode`
- 当业务准备从单向同步切换到双向同步时，建议单独评估过滤规则、控制面 key 前缀、同 key 双边写入语义和回滚方式

## 4. 使用建议

- 两个方向都必须部署 syncer，并且两边都开启 `bisyncEnabled: true`
- 灰度初期建议按业务前缀或 slot 范围控制同步范围
- 生产默认推荐 `mode: sync`
- 如果计划使用 `mode: pipeline`，应先完成故障恢复、failover 和长稳验证
- 业务侧必须提前定义同 key 双边写入时的冲突语义

## 5. 已知限制

### 5.1 一致性与冲突语义

- bisync 不解决业务冲突
- 如果两边同时写同一个 key，最终结果取决于 Redis 命令语义、复制顺序和恢复过程
- `INCR`、`LPUSH`、`XADD` 等非幂等命令应在业务侧先完成语义评估

### 5.2 cluster 约束

- cluster 模式下，单个 replay unit 必须能证明业务 key 属于同一个 slot
- 跨 slot 事务会按保守策略失败，而不是部分回放

### 5.3 命令支持范围

- RedisJSON、RedisBloom 等模块命令当前不纳入稳定发布门禁
- 无法通过 keyspec 或 `COMMAND GETKEYS` 证明 key 集合的命令，不建议进入生产 bisync 链路

### 5.4 控制面 key

以下命名空间属于 GunYu 控制面，业务侧不要读写、迁移、删除或复用：

- `redis-gunyu-bisync:*`
- `redis-gunyu-checkpoint*`
- `/redis-gunyu*`

## 6. 发布与上线建议

### 6.1 发布前

- 使用生产同版本 Redis 完成 bisync 发布测试
- 确认业务命令集合不包含未验证模块命令和无法解析 key 的命令
- 确认运维、监控和回滚方案已经准备完成

### 6.2 灰度上线

- 先以有限业务前缀、有限 slot 范围或有限业务流量启动
- 观察 syncer 状态接口、bisync 指标、Redis 资源和业务对账结果
- 重点关注 `bisync_txn_commit`、`bisync_txn_suppress`、`bisync_single_slot_fail`、`bisync_commit_backlog`

### 6.3 回滚建议

如果灰度阶段出现风险，可按以下方式快速回退：

- 关闭 `bisyncEnabled`
- 停止反向链路，只保留单向同步
- 按业务侧预案执行对账与修复

## 7. 相关文档

- [bisync.md](./bisync.md)
- [bisync_en.md](./bisync_en.md)
- [bisync_scheme_selection.md](./bisync_scheme_selection.md)
- [bisync_scheme1_impl.md](./bisync_scheme1_impl.md)
- [sync_configuration_zh.md](./sync_configuration_zh.md)
- [bisync_perf_report.md](./bisync_perf_report.md)
