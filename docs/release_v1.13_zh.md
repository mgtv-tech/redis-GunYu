# redis-GunYu v1.13 发布说明

## 1. 版本概述

`v1.13` 是基于 `v1.1.2` 的功能版本，主要引入了可配置的 channel backend，并新增 `memory channel` 实现。

本版本的重点不是调整同步协议，而是在保持现有 `input -> channel -> output` 主流程不变的前提下，为本地缓存层增加新的后端类型选择：

- `storer`：原有磁盘落盘缓存，默认行为
- `memory`：新的纯内存缓存，减少磁盘 I/O

如果你的场景更关注本地写盘开销和低延迟，并且可以接受本地缓存不跨进程重启保留，`v1.13` 可以作为一个更合适的版本选择。

## 2. 主要更新

### 2.1 新增 memory channel backend

- 新增 `channel.type: memory`
- 支持 full sync 的 RDB 缓存
- 支持 incremental sync 的 AOF 缓存
- 保持现有 `Channel` 抽象不变，对上层同步流程透明

`memory channel` 通过内存 segment 组织 RDB/AOF 数据，不再依赖本地磁盘文件作为缓存介质。

### 2.2 支持动态选择 channel 类型

- `channel.type` 现在支持 `storer` 和 `memory`
- 默认值仍为 `storer`，旧配置不填写 `channel.type` 时行为不变
- 配置校验和 runtime 初始化已支持按 backend 类型选择对应实现

这意味着已有部署默认不会因为升级而自动切换到新的内存模式，兼容性上更稳妥。

### 2.3 为 memory channel 增加容量控制

- 新增 `channel.memory.maxSize`
- 新增 `channel.memory.logSize`
- 当达到 `maxSize` 上限时，旧 segment 会按可回收状态逐步释放
- 内存上限是硬约束，不会因为缓存持续堆积而无限增长

这部分设计的目标是让 `memory channel` 在减少磁盘 I/O 的同时，仍然具备可控的资源边界。

### 2.4 补充测试与实现文档

- 新增 `syncer/memory_channel_test.go`
- 覆盖 RDB/AOF 写入读取、offset 范围、容量上限等核心行为
- 新增 `docs/memory_channel_impl_zh.md`，说明实现结构、并发语义、GC 与限制
- 同步更新中英文配置文档，补充 `channel.type: memory` 的配置说明

## 3. 配置变更

### 3.1 新增 channel 类型配置

```yaml
channel:
  type: memory
  memory:
    maxSize: 536870912
    logSize: 104857600
```

新增配置项如下：

- `channel.type`
  - `storer`：磁盘缓存，默认值
  - `memory`：内存缓存
- `channel.memory.maxSize`
  - 最大内存缓存空间
  - 默认 `512 MiB`
- `channel.memory.logSize`
  - 内存逻辑分段大小
  - 默认 `100 MiB`

### 3.2 旧配置兼容

- 如果不配置 `channel.type`，仍然使用 `storer`
- 原有 `channel.storer` 配置结构保持不变
- `v1.1.2` 的现有配置可直接沿用

## 4. 适用场景与限制

### 4.1 建议使用 memory channel 的场景

- 本地磁盘 I/O 是同步链路中的主要瓶颈
- 希望减少 RDB/AOF 本地落盘开销
- 更关注低延迟缓存链路
- 可以接受 channel 缓存为易失状态

### 4.2 当前限制

- `memory channel` 不保留跨进程重启的本地缓存
- 当 `maxSize` 不足以覆盖较长回放窗口时，旧 offset 可能失效
- 当 RDB 早期 segment 被回收后，该 RDB 将不再可用于完整 replay
- 不适合作为超大容量、长时间积压场景下的持久化本地缓存替代方案

因此，`memory channel` 更适合“减少本地写盘”和“控制内存上限”的场景，而不是替代 `storer` 的全部持久化语义。

## 5. 兼容性与升级说明

- `v1.13` 不改变默认 channel 行为
- 不升级配置时，系统仍按 `storer` backend 运行
- 升级到 `v1.13` 后，可按需逐步切换到 `memory` 模式
- 切换到 `memory` 模式前，应根据业务峰值流量评估 `maxSize`

建议升级后重点验证以下内容：

- full sync 与 incremental sync 是否均可正常推进
- `memory channel` 下 offset 是否持续前进
- 内存占用是否符合 `maxSize` 预期
- 重启后的恢复语义是否符合业务预期

## 6. 建议升级对象

如果你当前使用 `v1.1.2`，并且符合以下任一场景，建议评估升级到 `v1.13`：

- 需要降低本地磁盘写入压力
- 当前同步延迟与本地缓存落盘开销相关
- 希望在保持现有同步主流程不变的情况下切换缓存 backend
- 需要在 `storer` 与 `memory` 之间按场景选择 channel 实现

如果你的场景依赖本地缓存跨重启保留，或需要更长时间的本地积压窗口，仍建议继续使用默认的 `storer` 模式。

## 7. 相关文档

- 配置说明：[sync_configuration_zh.md](./sync_configuration_zh.md)
- 实现说明：[memory_channel_impl_zh.md](./memory_channel_impl_zh.md)
