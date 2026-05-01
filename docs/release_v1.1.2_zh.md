# redis-GunYu v1.1.2 发布说明

## 1. 版本概述

`v1.1.2` 是基于 `v1.1.1` 的补丁版本，聚焦修复 standalone 同步场景下 `SELECT` 命令对双位数 Redis DB 编号的编码错误。

本版本不引入新的功能开关，也不涉及配置格式变更。主要目标是修复 `db >= 10` 时的增量回放与断点续传失败问题，适合作为 `v1.1.1` 的直接升级版本。

## 2. 主要更新

### 2.1 修复双位数 DB 的 AOF 回放问题

- 修复输出侧 AOF 解析后重组 `SELECT` 命令时的 DB 编号编码错误
- 之前的实现仅适用于 `db 0..9`，当目标 DB 为 `10` 及以上时，可能将 DB 编号错误编码为单字节字符
- 在 standalone 到 standalone 的同步场景下，这会导致目标 Redis 返回 `ERR value is not an integer or out of range`

### 2.2 修复断点续传恢复到双位数 DB 的问题

- 修复 `resumeFromBreakPoint` 场景下恢复起始 DB 时的同类编码问题
- 当 checkpoint 记录的 DB 为 `10` 及以上时，重启后可正确恢复到对应 DB，而不会在首次 `SELECT` 时失败

### 2.3 补充回归测试

- 新增 `SELECT 10` 的 AOF 解析回归测试
- 新增断点续传起始 DB 为双位数时的回归测试
- 新增 `targetDbMap` 映射到双位数目标 DB 时的回归测试

## 3. 影响范围

受影响的主要是以下场景：

- 输出端为 standalone Redis
- AOF 增量回放过程中出现 `SELECT 10` 及以上 DB
- 或者断点续传恢复到 `db >= 10`
- 或者通过 `targetDb` / `targetDbMap` 将命令映射到 `db >= 10`

以下场景通常不受影响：

- 仅使用 `db 0..9` 的 standalone 同步
- 输出端为 Redis Cluster 的场景
- RDB 全量回放路径

## 4. 兼容性与升级说明

- `v1.1.2` 不引入配置破坏性变更
- `v1.1.1` 的配置可直接沿用
- 已经在 `db 10+` 受影响场景上补充回归测试，升级后无需调整已有参数

如果故障期间已经发生增量中断，建议升级后重点验证：

- syncer 是否能继续推进 offset
- 目标端 `db 10+` 是否恢复写入
- 日志中是否不再出现 `ERR value is not an integer or out of range`

## 5. 建议升级对象

如果你当前使用 `v1.1.1`，并且符合以下任一场景，建议优先升级到 `v1.1.2`：

- 业务使用了 `db 10` 及以上的 Redis 逻辑库
- 开启了 `resumeFromBreakPoint`
- 使用 `targetDb` 或 `targetDbMap` 做 DB 映射

## 6. 相关问题与提交

- Issue: `#108` `当redis同步数据库下标设置超过10后，数据就不同步了，是存在问题还是功能限制呢？`
