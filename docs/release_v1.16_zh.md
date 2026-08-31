# redis-GunYu v1.16 发布说明

## 1. 主要更新

- 源端和目标端新增原生 Redis Sentinel 拓扑发现支持。
- Sentinel 和 Redis 数据节点可以分别配置认证信息和 TLS。
- Sentinel 源端支持通过 `master`、`slave`、`prefer_slave` 选择同步节点；
  Sentinel 目标端始终解析并写入当前 master。
- 源端或目标端发生切换后，拓扑监控会重建受影响的 syncer，并复用现有复制 ID、
  channel 和 checkpoint 恢复流程。
- Sentinel HA 选举使用基于 `masterName` 的稳定身份，Redis master 物理地址变化后，
  两个 GunYu 实例仍竞争同一个逻辑源。
- 新增真实 Redis Sentinel 切换、安全矩阵、双 GunYu HA、兼容性以及升级回滚门禁。

## 2. 支持的拓扑和回放模式

单向同步的源端或目标端都可以使用 Sentinel。支持以下组合：

| 源端 | 目标端 | 状态 |
| --- | --- | --- |
| Sentinel | Standalone | 支持 |
| Standalone | Sentinel | 支持 |
| Sentinel | Sentinel | 支持 |
| Sentinel | Cluster | 支持 |
| Cluster | Sentinel | 支持 |

Sentinel 拓扑支持 `sync` 和 `pipeline` 回放。Sentinel 与
`bisyncEnabled: true` 组合会在配置校验阶段被拒绝。`parallel` 是 bisync 专用
回放模式，因此 Sentinel 源端或目标端不能使用 `parallel`。

## 3. 配置

当 `type: sentinel` 时，`addresses` 填写 Sentinel 地址，而不是 Redis 数据节点
地址。Redis 顶层认证信息和 `tlsEnable` 用于数据节点；`sentinelOptions` 仅用于
Sentinel 连接。

```yaml
input:
  redis:
    type: sentinel
    addresses:
      - 10.0.0.11:26379
      - 10.0.0.12:26379
      - 10.0.0.13:26379
    userName: data-user
    password: data-password
    tlsEnable: false
    sentinelOptions:
      masterName: source-redis
      userName: sentinel-user
      password: sentinel-password
      tlsEnable: false
  syncFrom: prefer_slave

output:
  redis:
    type: sentinel
    addresses:
      - 10.1.0.11:26379
      - 10.1.0.12:26379
      - 10.1.0.13:26379
    userName: data-user
    password: data-password
    tlsEnable: false
    sentinelOptions:
      masterName: target-redis
      userName: sentinel-user
      password: sentinel-password
      tlsEnable: false
  replay:
    mode: pipeline
```

`sentinelOptions.masterName` 和至少一个 Sentinel 地址为必填项。建议配置多个
Sentinel 地址，避免单个 Sentinel 不可用时无法继续发现拓扑。

## 4. 切换与一致性

GunYu 会定期解析 Sentinel 拓扑。选中的源节点或目标 master 发生变化时，GunYu
会停止受影响的 syncer，并使用新的物理地址重建。恢复时会通过现有复制 ID、本地
channel 和 checkpoint 尝试增量同步；无法增量同步时退化为全量同步。

Redis Sentinel failover 建立在异步复制之上。旧 master 已接受但尚未复制到新
master 的写入可能丢失。本版本仍采用最终一致/弱一致模型，不提供零 RPO、
exactly-once 回放或强一致性。切换后应校验业务值和 checkpoint，非幂等命令尤其
需要执行该校验。

## 5. 安全与部署要求

- Sentinel 和数据节点可以使用不同的 ACL 用户。
- Sentinel TLS 和数据节点 TLS 可以分别启用。
- Sentinel 返回的 master 和 replica 地址必须能从 GunYu 直接访问。本版本不执行
  NAT 或地址重写。
- Sentinel 使用 ACL 用户访问数据节点时，该用户必须具备 Sentinel 所需的命令、
  key 和 Pub/Sub channel 权限，包括 Sentinel hello channel 的访问权限。
- 仅 TLS 的 Sentinel 部署必须配置 GunYu 可访问的 `announce-ip` 和
  `announce-port`，并指向 TLS 端口。

## 6. GunYu HA 行为

Sentinel 源端的 GunYu HA 使用逻辑身份
`sentinel/<escaped-masterName>`，不再使用当前 Redis master 地址。因此 Redis
发生切换后选举 key 保持不变。资格测试已验证：一个 GunYu 保持 leader，另一个
保持 follower；持续写入期间停止 leader 后，follower 可以接管并完成数据收敛。

## 7. 兼容性与资格状态

- Redis 7.4.1 是本版本的发布合格基线。
- Redis 8.0.0 已通过 Sentinel `sync`、`pipeline`、持续写入和切换的核心兼容性
  测试。在完成要求的 soak 和 benchmark 门禁前，Redis 8 不属于耐久性合格版本。
- 现有 Standalone 和 Cluster 配置保持兼容。只有选择 `type: sentinel` 时才需要
  新增的 Sentinel 字段。
- 本版本不修改 checkpoint 或持久化 metadata schema。

本次已完成的非耐久发布资格结果：

- `make test-release` 通过，包括静态检查、全部单元测试、全仓 race、必要集成测试
  和 E2E smoke。
- 必要集成测试：241 项，0 失败、0 跳过、0 缺失。
- E2E smoke：7/7 通过。
- 覆盖率：33.5%，高于 30.5% 门槛。
- Redis 7.4.1 Sentinel ACL/TLS 四种组合全部通过。
- 使用保留的 v1.14 二进制执行 Redis 7.4.1 Sentinel 升级回滚，通过。
- Redis 7.4.1 和 8.0.0 的双 GunYu Sentinel HA 在持续写入下通过 `sync` 和
  `pipeline`，源端和目标端业务值精确一致。

本次资格验证按要求未执行性能、benchmark 和 soak 测试。当前有一个非阻断的
可观测性问题：follower 启动或接管期间的预期重试，可能在收敛前以 error 级别记录
`empty run id`、连接不可用或 reader error。

## 8. 升级与回滚

安全引入 Sentinel 的步骤：

1. 备份 GunYu 配置、本地 channel 目录和目标 Redis checkpoint key。
2. 保留原有直连地址配置升级二进制，先验证同步和 checkpoint 推进正常。
3. 同一 HA group 中任何实例启用 `type: sentinel` 前，停止所有仍使用旧版直连
   地址选举身份的 GunYu 实例。
4. 启动 Sentinel 配置的实例，通过日志或状态接口确认解析出的源端、目标端 master，
   并确认每个逻辑源只有一个 GunYu leader。
5. 恢复写入并校验业务值和 checkpoint。

不要让同一数据源的旧版直连 HA 实例与 Sentinel 模式实例同时运行。两者使用不同
选举身份，不能形成互斥。

回滚到不支持 Sentinel 的旧版本：

1. 从 Sentinel 查询当前源端和目标端 master 地址。
2. 停止 HA group 中所有 Sentinel 模式的 GunYu 实例。
3. 将两端 Redis 配置改为 `type: standalone`，把 `addresses` 替换为解析出的数据
   master 地址，并删除 `sentinelOptions`。
4. 启动旧版二进制，在恢复正常流量前校验业务值和 checkpoint 推进。

已验证的 v1.14 回滚可以复用现有 channel 和 checkpoint，无需迁移 metadata。

## 9. 已知限制

- 不支持 Sentinel 与 bisync 组合。
- 不支持使用 Sentinel 管理 Redis Cluster 的非标准拓扑。
- 通过定期解析拓扑检测切换，尚未订阅 Sentinel `+switch-master` Pub/Sub 通知。
- 直接使用 Sentinel 返回的地址，这些地址必须能从 GunYu 访问。
- Redis 异步切换可能丢失晋升前尚未复制的写入。

## 10. 相关文档

- 配置说明：[sync_configuration_zh.md](./sync_configuration_zh.md)
- 注意事项：[attentions_zh.md](./attentions_zh.md)
- 测试与兼容性状态：[test_zh.md](./test_zh.md)
