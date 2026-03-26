# 双向同步配置步骤（cluster <-> cluster）

## 适用前提

1. 源端与目标端都必须是 Redis Cluster（当前主同步链路仅支持 `cluster <-> cluster`）。
2. 双向同步通常需要启动两份 `redisGunYu` 配置（A->B 一份，B->A 一份），它们通过 `syncCheckPointKey` / `filterCheckPointKey` 交叉配置来防回环。
3. 必须配置 `output.metaRedis`（sentinel模式）作为协调/元数据后端。
4. （可选）开启审计（`audit`）将过滤命令/发送命令异步写入 ClickHouse（用于排障与对账）。
   - 详细说明见：`docs/clickhouse_audit.md`

## Step 1：准备两套集群与 metaRedis

- 集群 A：提供 `7001-7006`（示例端口，实际替换）
- 集群 B：提供 `8001-8010`（示例端口，实际替换）
- metaRedis：提供 sentinel（示例 `26379-26381`，实际替换）

metaRedis（sentinel）建议：
- `type: sentinel`
- `masterName` 必须填你的 sentinel 监控主名
- sentinel 端点必须能被运行 redisGunYu 的机器访问

## Step 2：确定 checkpoint 前缀（非业务 key 前缀）

为避免回环，两个方向必须“交叉配置”：

- 选择：
  - `syncCheckPointKey_A`：表示“链路 A->B 的 checkpoint 命名空间前缀”
  - `syncCheckPointKey_B`：表示“链路 B->A 的 checkpoint 命名空间前缀”

- 对 A->B 这份配置：
  - `input.syncCheckPointKey = syncCheckPointKey_A`
  - `input.filterCheckPointKey = syncCheckPointKey_B`

- 对 B->A 这份配置：
  - `input.syncCheckPointKey = syncCheckPointKey_B`
  - `input.filterCheckPointKey = syncCheckPointKey_A`

注意：
- 这两个前缀都必须是“非业务”前缀，避免与真实业务 key 重名，导致业务key被误过滤。
- 只要发生双向同步，就必须交叉配置，回环过滤依赖checkpoint，否则可能出现无法正确避免回环。

## Step 3：配置 A->B 的 redisGunYu（cluster A -> cluster B）

建议你创建类似 `config/cluster2-runtime-a.yaml` 的配置，并只改你自己的地址/端口/前缀。

关键字段解释（只列“必改/最重要”）：
- `server.listen` / `server.listenPeer`：两份配置必须端口不同
- `input.redis.addresses`：指向 cluster A
- `output.redis.addresses`：指向 cluster B
- `input.syncCheckPointKey` / `input.filterCheckPointKey`：交叉配置（见 Step 2）
- `output.metaRedis`：sentinel（建议两份配置都指向同一个 metaRedis）
- `channel.storer.dirPath`：两份配置必须不同目录，避免互相覆盖缓存文件
- `output.replay.resumeFromBreakPoint`：建议保持默认 `true`（便于断点续传）
- `cluster.groupName` / `cluster.leaseTimeout`：多实例协调参数（示例两份都用同一个 groupName）

示例（参考 `config/cluster2-runtime-a.yaml` 配置）：

```yaml
server:
  listen: 127.0.0.1:18012
  listenPeer: 127.0.0.1:18012

input:
  redis:
    addresses: [127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005,127.0.0.1:7006]
    password: <redis密码>
    type: cluster
  mode: dynamic
  syncFrom: prefer_slave
  syncCheckPointKey: redis-GunYu-Checkpoint-ClusterA
  filterCheckPointKey: redis-GunYu-Checkpoint-ClusterB

channel:
  storer:
    dirPath: /tmp/redisgunyu-cluster2-runtime-a

output:
  redis:
    addresses: [127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003,127.0.0.1:8004,127.0.0.1:8005,127.0.0.1:8006,127.0.0.1:8007,127.0.0.1:8008,127.0.0.1:8009,127.0.0.1:8010]
    password: <redis密码>
    type: cluster
  metaRedis:
    addresses: [127.0.0.1:26379,127.0.0.1:26380,127.0.0.1:26381]
    password: <redis密码>
    type: sentinel
    masterName: mymaster
  replay:
    resumeFromBreakPoint: true

cluster:
  groupName: AtoB
  leaseTimeout: 9s
```

## Step 4：配置 B->A 的 redisGunYu（cluster B -> cluster A）

创建类似 `cluster2-runtime-b.yaml` 的配置，和 Step 3 的差异只有：
1. `input.redis.addresses` / `output.redis.addresses` 需要互换（B->A）
2. `input.syncCheckPointKey` / `input.filterCheckPointKey` 需要互换（交叉配置）
3. `server.listen` / `channel.storer.dirPath` 必须不同（避免端口/缓存冲突）
4. 配置反向同步，开启参数 `skipReplyRdb` 跳过 RDB 回放，直接使用 AOF 进入增量同步

示例（参考 `config/cluster2-runtime-b.yaml` 配置）：

```yaml
server:
  listen: 127.0.0.1:18013
  listenPeer: 127.0.0.1:18013

input:
  redis:
    addresses: [127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003,127.0.0.1:8004,127.0.0.1:8005,127.0.0.1:8006]
    password: <redis密码>
    type: cluster
  mode: dynamic
  syncFrom: prefer_slave
  syncCheckPointKey: redis-GunYu-Checkpoint-ClusterB
  filterCheckPointKey: redis-GunYu-Checkpoint-ClusterA
  skipReplyRdb: true

channel:
  storer:
    dirPath: /tmp/redisgunyu-cluster2-runtime-b

output:
  redis:
    addresses: [127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005,127.0.0.1:7006]
    password: <redis密码>
    type: cluster
  metaRedis:
    addresses: [127.0.0.1:26379,127.0.0.1:26380,127.0.0.1:26381]
    password: <redis密码>
    type: sentinel
    masterName: mymaster
  replay:
    resumeFromBreakPoint: true

cluster:
  groupName: BtoA
  leaseTimeout: 9s
```

## Step 5：启动与验证

启动（示例）：
1. 启动 A->B：
   - `./redisGunYu -conf ./config/cluster2-runtime-a.yaml -cmd=sync`
2. 启动 B->A：
   - `./redisGunYu -conf ./config/cluster2-runtime-b.yaml -cmd=sync`

验证：
1. 访问每个进程的状态接口：
   - `curl http://<server.listen>/syncer/status`
2. 在 cluster A、cluster B 分别写入业务 key：
   - 观察另一侧能否被同步
3. 如果写入带有错误的 checkpoint 前缀（例如业务 key 和 checkpoint 前缀冲突），可能导致误过滤：
   - 这就是 Step 2 的前缀要求为什么重要。

## 常见坑与建议

1. 忘记交叉配置：
   - `filterCheckPointKey` 如果不指向对端的 `syncCheckPointKey`，回环风险会显著上升。
2. `output.metaRedis` 没配置或与业务端点混用：
   - 必须是 sentinel，且与 `output.redis` 隔离。
3. 两份配置复用同一个 `channel.storer.dirPath`：
   - 会互相覆盖本地缓存和断点信息，建议两份配置使用不同目录。
4. `server.listen` 相同：
   - 两个进程的 HTTP 服务端口必须不同。
5. 该方案无法进行冲突检测。业务需要避免两边redis同时操作相同的key，导致出现数据不一致。

