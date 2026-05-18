# Bisync 双向同步自动化测试设计

## 1. 文档目标

本文用于回答两个问题：

- `bisync` 双向同步如果要上线，必须做哪些测试。
- 这些测试如何落到当前仓库的 `/tests` 目录，并形成可执行的自动化门禁。

本文以当前仓库实现为准，重点覆盖 cluster 双向同步；standalone 双向同步作为兼容项单列。

## 2. 当前现状与结论

### 2.1 已有测试资产

当前仓库已经有一套 `tests/bisync` 测试骨架：

- [tests/bisync/run_category1.sh](../../tests/bisync/run_category1.sh)：基础双向收敛
- [tests/bisync/run_category2.sh](../../tests/bisync/run_category2.sh)：重启、断点续传、`sync`/`pipeline`/`parallel`
- [tests/bisync/run_category3.sh](../../tests/bisync/run_category3.sh)：RDB 特殊路径与纯全量同步
- [tests/bisync/run_category4.sh](../../tests/bisync/run_category4.sh)：keyspec、过滤、路由
- [tests/bisync/run_category5.sh](../../tests/bisync/run_category5.sh)：failover、拓扑扰动、syncer restart

### 2.2 当前上线阻塞项

当前 `run_category1/2/3/5.sh` 生成的配置已经显式设置：

- `replayTransaction: true`
- `mode: sync|pipeline|parallel`
- `output.replay.bisyncEnabled: true`

`mode` 统一描述 AOF 回放执行语义；非 bisync 下 `pipeline` 等价于旧 pipeline，bisync 下 `sync/pipeline/parallel` 分别覆盖三种 checkpoint/recovery 语义。

结论：

- 当前测试目录的分类方向是对的。
- 现有脚本已显式启用 bisync 主路径。
- 上线门禁需要继续覆盖压力、兼容性、灰度回滚相关测试。

## 3. 测试目标

bisync 上线前必须证明以下几点：

- 正确性：双向写入最终收敛，不出现数据丢失、重复执行、错误覆盖。
- 回环抑制：A -> B 的镜像写不会再被 B -> A 回流。
- 恢复性：syncer 重启、源端切主、目标端切主后可以继续同步，且恢复点正确。
- 一致性：`sync`、`pipeline` 和 `parallel` 三种模式都满足设计约束。
- 控制面正确：`latest`、`commit`、`frontier`、`rdb` 元数据形态与设计一致。
- 路由安全：cluster 下真实 key 解析、同 slot 校验、`MOVED/ASK` 处理正确。
- 边界安全：RDB 全量同步、split key、`keyExists` 策略、过滤投影等边界行为正确。
- 可上线性：在长时间运行和大流量下，性能、资源占用、日志信号、回归风险可接受。

## 4. 风险清单

bisync 的高风险点主要有：

- 非幂等命令重复执行，例如 `INCR`、`LPUSH`、`XADD`
- 真实事务边界被破坏，导致 mirrored transaction 识别失败
- `parallel` 模式恢复点推进错误，造成漏放或重放
- cluster 拓扑变化后 metadata 找不到或提交到错误节点
- `COMMAND GETKEYS` / static keyspec 解析不一致，导致错误路由
- RDB 全量同步和 AOF 增量同步的边界混淆
- `keyExists=replace|ignore|error` 在 bisync 路径语义不一致
- 升级、回滚、灰度期间 checkpoint namespace 迁移异常

## 5. 测试分层

| 层级 | 目标 | 当前入口 | 上线要求 |
| --- | --- | --- | --- |
| 单元测试 | 校验 key 解析、checkpoint/frontier、replay unit 组装、RDB 特殊路径 | `go test ./...` 中的 `syncer`、`pkg/filter`、`pkg/redis/checkpoint`、`pkg/redis/client/cluster` | PR 必跑 |
| 组件测试 | 校验单模块对 Redis 协议与 cluster 行为的约束 | `tests/bisync/run_category4.sh` | PR 必跑 |
| 集成测试 | 校验双向链路收敛、恢复、metadata 形态 | `run_category1/2/3.sh` | 合并前必跑 |
| 故障测试 | 校验 failover、重定向、syncer restart | `run_category5.sh` | 合并前必跑 |
| 压测/长稳 | 校验吞吐、时延、资源、长时间运行稳定性 | 需新增 `run_category6.sh` | 发布前必跑 |
| 兼容性测试 | 校验 Redis 版本、模块、升级/回滚、standalone | 需新增 `run_category7.sh` | 发布前必跑 |

## 6. 上线必测矩阵

### 6.1 P0：上线阻断项

| 编号 | 场景 | 核心检查点 | 自动化建议 | 当前状态 |
| --- | --- | --- | --- | --- |
| P0-01 | bisync 开关生效 | `bisyncEnabled=true` 时走 bisync 主路径，`false` 时不走 | 配置回归测试 + `category1/2/3/5` | 已覆盖 |
| P0-02 | 基础双向收敛 | 双端混合写入后业务 key 完全一致 | `run_category1.sh` | 已覆盖核心样例 |
| P0-03 | 非幂等命令安全 | `INCR`、`LIST`、事务内多命令不重复、不丢失 | `run_category1.sh`、`run_category2.sh` | 已覆盖核心样例 |
| P0-04 | `sync` 模式断点续传 | 停 syncer、离线写入、重启后恢复，`latest` 存在且 `commit/frontier` 不残留 | `run_category2.sh` | 已覆盖核心样例 |
| P0-05 | `pipeline`/`parallel` 模式断点续传 | 重启恢复后 `frontier` 存在，`commit` 最终清零 | `run_category2.sh` | 已覆盖核心样例 |
| P0-06 | 纯 RDB 全量同步边界 | 只允许短生命周期 marker / full-sync barrier，不提前生成 authoritative `latest/commit/frontier` | `run_category3.sh` | 已覆盖核心样例 |
| P0-07 | key 解析与 strict 路由 | static keyspec、`COMMAND GETKEYS`、cross-slot 拒绝行为正确 | `run_category4.sh` | 已覆盖 |
| P0-08 | 源端 failover | 源 cluster 切主后双向链路继续同步，业务数据不分叉 | `run_category5.sh` | 已覆盖：双向 syncer、双边写入、`sync`/`pipeline`/`parallel` |
| P0-09 | 目标端 failover | 目标 cluster 切主、`MOVED/ASK` 后双向 replay unit 仍能提交 | `run_category5.sh` | 已覆盖：双向 syncer、双边写入、`sync`/`pipeline`/`parallel` |
| P0-10 | syncer restart | 通过 API 重启两条 syncer 后恢复正常，日志有拓扑/重启信号 | `run_category5.sh` | 已覆盖：双向 syncer、双边写入、`sync`/`pipeline`/`parallel` |
| P0-11 | checkpoint namespace 迁移 | 旧 namespace 迁移、新 runID 续跑正确 | 补充 `go test ./syncer -run CheckpointNamespace` 到 runner | 单测已存在，runner 未接入 |
| P0-12 | 关闭 bisync 的回归安全 | 普通单向同步/非 bisync cluster 事务路径不被这次改动破坏 | 新增非 bisync 回归脚本或 CI job | 未覆盖 |

### 6.2 P1：发布前必做但可晚于功能自测

| 编号 | 场景 | 核心检查点 | 自动化建议 |
| --- | --- | --- | --- |
| P1-01 | 长稳运行 | 连续运行 4 到 24 小时不出现收敛失败、goroutine 泄漏、磁盘堆积 | 新增 `run_category6.sh` |
| P1-02 | 高并发写入压力 | 多线程混合写、较大 value、热点 slot 与多 slot 并存下保持收敛 | 新增 `run_category7.sh` |
| P1-03 | 大 key / 大事务 | 大 hash、长 list、批量事务、RDB restore 边界无异常 | 新增 `run_category6.sh` |
| P1-04 | 过滤规则回归 | key 前缀白黑名单、slot 白黑名单、事务部分投影结果正确 | 扩充 `run_category4.sh` 与 `syncer` 单测 |
| P1-05 | `keyExists` 策略矩阵 | `replace`、`ignore`、`error` 在 AOF/RDB 路径行为一致 | 扩充 `run_category3.sh` |
| P1-06 | 监控与日志 | 指标、状态接口、错误日志可定位恢复点与失败原因 | 增加脚本断言与日志 grep |
| P1-07 | standalone 兼容性 | standalone -> standalone 双向同步与 cluster 路径隔离 | 新增 standalone runner |

### 6.3 P2：灰度与版本演进建议项

| 编号 | 场景 | 核心检查点 | 自动化建议 |
| --- | --- | --- | --- |
| P2-01 | Redis 版本矩阵 | 7.0、7.2、更新版本 keyspec 行为一致 | 新增 `run_category7.sh` |
| P2-02 | 模块命令矩阵 | RedisJSON、RedisBloom、RediSearch 等模块命令 key 解析正确 | 复用 `keyspec_verify` 内置模块样本，并可通过 `--samples-file` 扩展生产命令集 |
| P2-03 | 升级/回滚 | 旧版本 checkpoint + 新版本 bisync 恢复正确；回滚后不破坏数据 | 新增升级回归 runner |
| P2-04 | 异常注入 | 目标端写失败、网络抖动、磁盘空间紧张时 fail-stop 行为清晰 | 新增故障注入脚本 |

当前模块命令状态说明：

- 已为 `JSON.SET`、`JSON.DEL`、`JSON.MSET`、`BF.ADD`、`CMS.MERGE`、`TDIGEST.MERGE`、`TOPK.ADD`、`FT.CREATE`、`FT.SEARCH`、`FT.DROPINDEX` 建立静态 keyspec 和 `keyspec_verify` 样本。
- 这些样本只证明命令路由所需的 key 解析能力；模块自定义 RDB 数据类型仍需要单独验证。
- 已新增 `tests/bisync/run_category10.sh` 作为 Redis Modules 回归脚本，覆盖 Redis Stack `keyspec`、RedisJSON / RedisBloom 的真实 RDB 恢复，以及 `moduleAuxPolicy=fail|skip` 边界。
- 后续需要继续扩展生产实际使用的模块命令样本，并根据生产模块版本补版本矩阵。

## 7. 对现有 `tests/bisync` 的落地建议

### 7.1 保留并修正现有分类

建议保留当前 `category1-5` 的划分，不需要推翻重来。

建议动作：

- `category1`：作为基础正确性冒烟
- `category2`：作为 `sync`/`pipeline`/`parallel` 断点续传主门禁
- `category3`：作为 RDB 路径主门禁
- `category4`：作为 key 解析、过滤、cluster 路由主门禁
- `category5`：作为故障与拓扑扰动主门禁

### 7.2 必须立即补的改动

1. `run_category1/2/3/5.sh` 生成配置时显式加入 `output.replay.bisyncEnabled: true`。
2. 给 `run_category5.sh` 增加对 `MOVED/ASK` 或 typology 刷新的更明确日志断言。
3. 将 `syncer/checkpoint_namespace_test.go` 中的迁移测试纳入某个 runner。
4. 增加一个非 bisync 回归 job，确保本次功能不会破坏原有同步链路。

### 7.3 建议新增的脚本

建议新增两个类别：

- `tests/bisync/run_category6.sh`
  目标：多数据结构、大数据结构、外部 cluster 集成报告
- `tests/bisync/run_category7.sh`
  目标：长稳、持续写入、资源与收敛监控

- `tests/bisync/run_category9.sh`
  目标：发布前耐久长稳，按 `SOAK_TIER=2h|4h|6h` 分档手动推进；每档只运行一次并输出报告，报告认可后再启动下一档。脚本自建带 replica 且开启 AOF 的临时 cluster，在持续写入期间注入 syncer restart、Redis failover、syncer 离线恢复，并采集 RSS、goroutine、Redis memory、storer 目录等资源样本。

## 8. 执行环境建议

### 8.1 本地开发环境

如果你的本地已经准备好了可用的 Redis 可执行文件，并且 `c1`、`c2` 这类外部 cluster 已启动，这个环境适合做两类事情：

- 外部 Redis keyspec 校验
- 基础 smoke 测试或人工排查

例如：

- 可以把 `run_category4.sh` 指向外部 cluster 地址执行 `keyspec_verify`
- 如果 `c1/c2` 分别暴露为 `7000-7002`、`7100-7102`，可使用 `KEYSPEC_VERIFY_ADDRS`

说明：

- `category1/2/3/5` 当前脚本会自建临时 cluster，并主动 `shutdown` 指定端口，不建议直接复用你长期运行的 `c1/c2`
- failover、restart、故障注入类场景应始终使用临时集群，避免污染开发环境

### 8.2 CI / 发布环境

建议拆成三层门禁：

- PR 门禁：相关单测 + `run_category4.sh`
- 合并前门禁：`run_category1.sh` 到 `run_category5.sh`
- 发布前门禁：新增 `run_category6.sh`、`run_category7.sh`

## 9. 通过标准

bisync 上线建议采用以下通过标准：

- 所有 P0 用例通过
- 所有 P1 用例通过
- P2 至少完成版本矩阵中的目标线上 Redis 版本
- `sync`/`pipeline`/`parallel` 三种模式都通过，不允许只上线一种模式而另一种未验证
- 没有残留的 `commit` journal key
- metadata 形态与设计一致
- 断点续传日志中存在非零恢复 offset
- 故障注入后业务 key 最终一致，且非幂等命令没有重复放大

## 10. 推荐执行顺序

推荐按以下顺序推进自动化：

1. 先修正 `category1/2/3/5` 的 `bisyncEnabled` 配置。
2. 跑完全部相关单测与 `category4`，保证 key 解析与路由稳定。
3. 跑 `category1/2/3/5`，完成 bisync 主路径正确性、恢复性、故障性验证。
4. 补 `category6/7`，完成压测、兼容性、升级回滚验证。
5. 将 P0/P1 固化到 CI 或发布前 checklist。

## 11. 最终建议

如果目标是“可以上线 bisync”而不是“先做一版本地自测”，那么测试门禁最低要求不是一个脚本，而是一套组合：

- 单测
- 路由/keyspec 组件测试
- 基础收敛
- 断点续传
- RDB 边界
- failover/拓扑扰动
- 非 bisync 回归
- 压测与兼容性

其中最关键的一条是：

- 先确保自动化脚本真的在跑 `bisyncEnabled=true` 的主路径，否则所有结果都不能作为上线依据。
