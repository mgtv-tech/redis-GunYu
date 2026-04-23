# redis-GunYu v1.1.1 发布说明

## 1. 版本概述

`v1.1.1` 是基于 `v1.1.0` 的补丁版本，聚焦于 Redis 回包错误检测、实例角色识别准确性，以及发布前测试覆盖的补强。

本版本没有引入新的大功能开关，主要目标是提升同步链路在异常场景下的可观测性、可靠性和发布信心，适合作为 `v1.1.0` 的直接升级版本。

## 2. 主要更新

### 2.1 Redis 回包错误检测增强

- 新增 `CheckReplyError`、`CheckRepliesError`、`CheckTxnRepliesError`，统一校验单条命令、批量命令和事务命令的 Redis 回包
- 强化事务回放路径中的错误识别，能够更早发现 `MULTI/EXEC` 过程中返回的异常结果
- 改进 transaction batcher 和输出侧相关测试，减少“请求已发送但错误未被及时识别”的风险

### 2.2 Redis 角色识别与探测改进

- 重构 `GetRedisRoleOnline` 相关逻辑，拆分角色解析流程，提升代码可维护性
- 改进 standalone 和 cluster 场景下的角色探测准确性，降低误判和误报概率
- 补充角色识别测试用例，并增加 bisync 控制面场景下的回归验证

### 2.3 测试与发布门禁补强

- 为 non-bisync 场景增加 bulk dataset 生成与校验能力，扩大数据量和样本覆盖
- 增强 rich workload 测试，支持多组 key 集合的验证
- 新增 Redis 回包错误检测的集成测试
- 提升 Redis cluster 选主测试稳定性，减少测试之间的相互干扰
- 新增 `tests/bisync/run_controlplane_etcd.sh`，用于覆盖 bisync 在 etcd control plane 下的发布前验证

## 3. 兼容性与升级说明

- `v1.1.1` 不引入新的配置破坏性变更
- `v1.1.0` 的配置可直接沿用，无需因本次升级调整发布参数
- 已在测试与代码层面重点覆盖以下风险点：
  - Redis 命令执行成功但回包中携带错误
  - standalone / cluster 角色探测误判
  - bisync 在 etcd control plane 下的基础回归

## 4. 建议升级对象

如果你当前使用 `v1.1.0`，并且符合以下任一场景，建议优先升级到 `v1.1.1`：

- 希望更早发现事务或批量回放中的 Redis 返回错误
- 使用 standalone 与 cluster 混合环境，关注角色识别稳定性
- 正在准备 bisync 的发布验证，尤其是需要覆盖 etcd control plane 的场景

## 5. 发布建议

正式发布前，建议至少补跑以下验证项：

- 当前生产 Redis 版本下的基础同步回归
- 涉及事务、批量命令和异常回包的回放验证
- bisync 场景下的 control plane 回归，尤其是 etcd 模式

## 6. 相关提交

- `89cbd9c` `feat(redis): add reply error checking and bulk dataset testing (#105)`
- `0f7db05` `refactor(redis): extract role parsing logic and improve role detection (#107)`
