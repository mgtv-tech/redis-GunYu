# Checkpoint 机制与 Redis Key 说明

- [Checkpoint 机制与 Redis Key 说明](#checkpoint-机制与-redis-key-说明)
  - [1. 文档目的](#1-文档目的)
  - [2. 总体视图](#2-总体视图)
  - [3. 非 bisync 的 checkpoint key](#3-非-bisync-的-checkpoint-key)
    - [3.1 `redis-gunyu-checkpoint-hash`](#31-redis-gunyu-checkpoint-hash)
    - [3.2 `checkpointName`](#32-checkpointname)
    - [3.3 非 bisync 启动时如何恢复](#33-非-bisync-启动时如何恢复)
    - [3.4 非 bisync 运行时如何更新](#34-非-bisync-运行时如何更新)
  - [4. bisync 的 checkpoint key](#4-bisync-的-checkpoint-key)
    - [4.1 bisync namespace root: `checkpointName`](#41-bisync-namespace-root-checkpointname)
    - [4.2 `checkpointName:frontier`](#42-checkpointnamefrontier)
    - [4.3 `marker:{slotTag}`](#43-markerslottag)
    - [4.4 `latest:{slotTag}`](#44-latestslottag)
    - [4.5 `commit:{slotTag}:<unitSeq>`](#45-commitslottagunitseq)
    - [4.6 `index:{slotTag}`](#46-indexslottag)
    - [4.7 `rdb:{slotTag}:<unitSeq>`](#47-rdbslottagunitseq)
  - [5. bisync sync：启动恢复与运行更新](#5-bisync-sync启动恢复与运行更新)
    - [5.1 启动恢复](#51-启动恢复)
    - [5.2 运行更新](#52-运行更新)
  - [6. bisync pipeline/parallel：启动恢复与运行更新](#6-bisync-pipelineparallel启动恢复与运行更新)
    - [6.1 启动恢复](#61-启动恢复)
    - [6.2 运行更新](#62-运行更新)
  - [7. 启动时 checkpoint namespace 的创建与迁移](#7-启动时-checkpoint-namespace-的创建与迁移)
  - [8. 删除与 GC](#8-删除与-gc)
  - [9. 关键结论](#9-关键结论)

## 1. 文档目的

本文整理 `redis-GunYu` 当前代码里的 checkpoint 相关 Redis key，重点回答下面几个问题：

- 非 bisync 和 bisync 分别有哪些 key
- 每个 key 的类型、字段和值是什么
- 启动时如何从这些 key 还原 checkpoint
- 运行过程中如何更新这些 key
- key 之间是什么关系，谁才是 authoritative checkpoint

本文对应的主要实现文件：

- `config/var.go`
- `pkg/redis/checkpoint/checkpoint.go`
- `pkg/redis/checkpoint/checkpoint_info.go`
- `pkg/redis/checkpoint/bisync.go`
- `syncer/output.go`
- `syncer/syncer.go`
- `syncer/bisync.go`
- `syncer/bisync_rdb.go`

补充说明：

- 本文会保留 `parallel` 相关 checkpoint / frontier / journal 说明，因为代码实现尚未移除这条内部路径
- 但根据当前性能测试结果，`parallel` 没有体现稳定性能优势，因此默认模式仍建议优先 `sync`，追求吞吐时再按场景评估 `pipeline`

## 2. 总体视图

无论是否开启 bisync，checkpoint 相关 key 大体都分成两层：

1. 索引层

   用 `runId -> checkpointName` 找到当前 source 对应的 checkpoint namespace。

   固定 key 为：

   ```text
   redis-gunyu-checkpoint-hash
   ```

2. 数据层

   真实恢复点数据存放在 `checkpointName` 以及它衍生出的 key 中。

区别在于：

- 非 bisync：恢复点主要就是普通 `checkpointName` hash 里的 `runId_offset`
- bisync `sync`：恢复点主要来自各 slot 的 `latest`
- bisync `pipeline`/`parallel`：恢复点主要来自 `frontier + commit journal`

代码中常量定义：

```go
CheckpointKey        = "redis-gunyu-checkpoint"
CheckpointKeyHashKey = "redis-gunyu-checkpoint-hash"
```

## 3. 非 bisync 的 checkpoint key

非 bisync 指普通同步链路，即没有启用 `output.replay.bisyncEnabled`。

### 3.1 `redis-gunyu-checkpoint-hash`

key：

```text
redis-gunyu-checkpoint-hash
```

类型：

```text
HASH
```

用途：

- 建立 `runId -> checkpointName` 的映射
- 启动恢复时，先通过这个 key 找到当前 source runId 对应的 checkpoint root

例子：

```redis
HSET redis-gunyu-checkpoint-hash r1 redis-gunyu-checkpoint
```

表示：

- 当前 source 的 `runId = r1`
- 它对应的 checkpoint root key 是 `redis-gunyu-checkpoint`

### 3.2 `checkpointName`

非 bisync 下，`checkpointName` 可能是两种形态：

- standalone 或普通场景

  ```text
  redis-gunyu-checkpoint
  ```

- cluster 且当前事务模式需要选择一个落在目标 slot 范围内的 key 时

  ```text
  redis-gunyu-checkpoint-xxxxx
  ```

  这个后缀是运行时挑出来的，使该 key 能落到当前允许的 slot 范围。

类型：

```text
HASH
```

字段组织方式：

- `<runId>_runid`
- `<runId>_version`
- `<runId>_offset`
- `<runId>_mtime`

例子：

```redis
HSET redis-gunyu-checkpoint \
  r1_runid r1 \
  r1_version 1 \
  r1_offset 123456 \
  r1_mtime 1710000000000000000
```

含义：

- `r1_offset=123456` 表示当前 source `runId=r1` 已经安全同步到 offset `123456`
- `mtime` 是最近一次更新时间

这里的 `offset` 是非 bisync 链路最核心的恢复点。

### 3.3 非 bisync 启动时如何恢复

非 bisync 的启动恢复流程如下：

1. 读取 source 当前的 `runId` 或 `runId/runId2`
2. 在 `redis-gunyu-checkpoint-hash` 里查到对应的 `checkpointName`
3. 读取 `checkpointName` 这个 hash
4. 找到与当前 `runId` 匹配的 `<runId>_offset`
5. 以这个 offset 作为增量同步起点

示例：

Redis 中已有：

```redis
HSET redis-gunyu-checkpoint-hash r1 redis-gunyu-checkpoint
HSET redis-gunyu-checkpoint \
  r1_runid r1 \
  r1_version 1 \
  r1_offset 123456 \
  r1_mtime 1710000000000000000
```

启动时：

1. source 返回当前 `runId = r1`
2. GunYu 执行：

   ```redis
   HGET redis-gunyu-checkpoint-hash r1
   ```

   得到：

   ```text
   redis-gunyu-checkpoint
   ```

3. 再执行：

   ```redis
   HGETALL redis-gunyu-checkpoint
   ```

4. 解析出：

   ```text
   r1_offset = 123456
   ```

5. 最终从 source offset `123456` 继续同步

补充：

- standalone 输出端会扫描多个 DB，选择 offset 最大、mtime 较新的那份 checkpoint
- cluster 输出端只看 DB 0

### 3.4 非 bisync 运行时如何更新

非 bisync 下有两类更新方式。

#### 3.4.1 full sync 结束后更新

RDB 回放全部完成后，会写一次普通 checkpoint。

示例：

```redis
HSET redis-gunyu-checkpoint \
  r1_runid r1 \
  r1_version 1 \
  r1_offset 500000 \
  r1_mtime 1710001000000000000
```

表示：

- full sync 已完整落地到目标端
- 后续增量同步可以从 `offset=500000` 开始

#### 3.4.2 AOF 回放时更新

普通 AOF 回放有两种模式。

1. 非事务模式

   业务命令先执行，收到回复后按定时器刷新 checkpoint offset。

   示例：

   ```redis
   SET user:1 v1
   SET user:2 v2
   HSET redis-gunyu-checkpoint r1_offset 500123
   ```

   注意：

   - 这里 checkpoint 更新和业务命令不在同一个事务边界
   - 所以只能说这是“尽量靠后的恢复点”

2. 事务模式

   业务命令与 checkpoint 更新放在同一个 `MULTI/EXEC` 里。

   示例：

   ```redis
   MULTI
   SET user:1 v1
   SET user:2 v2
   HSET redis-gunyu-checkpoint r1_runid r1 r1_version 1
   HSET redis-gunyu-checkpoint r1_offset 500123
   EXEC
   ```

   这样语义更强：

   - 业务命令成功
   - checkpoint 一定也同步推进到对应 offset

## 4. bisync 的 checkpoint key

bisync 仍然使用 `redis-gunyu-checkpoint-hash` 作为索引层，但数据层会扩展成一个稳定 namespace。

典型 `checkpointName` 长这样：

```text
redis-gunyu-checkpoint-bisync:aa11bb22cc33dd44ee55ff66
```

它不是一次拓扑临时计算出的 shard key，而是一个稳定 namespace root。

### 4.1 bisync namespace root: `checkpointName`

例子：

```text
redis-gunyu-checkpoint-bisync:aa11bb22cc33
```

类型：

```text
HASH
```

里面存两类数据。

第一类：普通 shared checkpoint 字段

- `<runId>_runid`
- `<runId>_version`
- `<runId>_offset`
- `<runId>_mtime`

第二类：bisync namespace 元数据

- `bisync_mode`
- `bisync_mode_mtime`

例子：

```redis
HSET redis-gunyu-checkpoint-bisync:aa11bb22cc33 \
  r1_runid r1 \
  r1_version 1 \
  r1_offset 123456 \
  r1_mtime 1710000000000000000 \
  bisync_mode parallel \
  bisync_mode_mtime 1710000001000000000
```

这里的 root key 有三个作用：

1. 给整个 bisync namespace 提供稳定根名
2. 保存当前 namespace 属于 `sync`、`pipeline` 还是 `parallel`
3. 保留一个 shared checkpoint offset 作为 barrier 或迁移 seed

但要注意：

- 在 bisync AOF 恢复中，root key 里的 `runId_offset` 通常不是最终 authoritative 恢复点
- 真正 authoritative 的恢复点，在 `sync` 模式来自 `latest`，在 `pipeline`/`parallel` 模式来自 `frontier + commit journal`

### 4.2 `checkpointName:frontier`

key：

```text
<checkpointName>:frontier
```

例子：

```text
redis-gunyu-checkpoint-bisync:aa11bb22cc33:frontier
```

类型：

```text
HASH
```

字段：

- `version`
- `run_id`
- `unit_seq`
- `end_offset`
- `mtime`

例子：

```redis
HSET redis-gunyu-checkpoint-bisync:aa11bb22cc33:frontier \
  version 1 \
  run_id r1 \
  unit_seq 88 \
  end_offset 123456 \
  mtime 1710000002000000000
```

含义：

- `parallel` 模式下，当前已经连续确认到 `unit_seq=88`
- 它对应 source offset `123456`
- 这个点之前的 unit 都已经被 authoritative 地并入恢复面

`frontier` 是 namespace-global key，不是某个 slot 私有状态。

### 4.3 `marker:{slotTag}`

key：

```text
redis-gunyu-bisync:<checkpointName>:marker:{slotTag}
```

类型：

```text
STRING
```

值：

- 一个 JSON 字符串
- 带 TTL

字段：

- `record_type`
- `version`
- `run_id`
- `syncer_id`
- `unit_seq`
- `start_offset`
- `end_offset`
- `slot`
- `digest`

例子：

```redis
SET redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11bb22cc33:marker:{slot-8338-x} \
  '{"version":"1","run_id":"r1","syncer_id":"syncer-a","unit_seq":9,"start_offset":10,"end_offset":20,"slot":8338,"digest":"deadbeef"}' \
  PX 86400000
```

作用：

- 放在 mirrored transaction 的开头
- 接收端反向 parser 看到它，就知道这笔事务不是业务原生写，而是 GunYu 镜像事务
- 从而整笔事务抑制，不再反向回放

`marker` 的职责是回环抑制，不是恢复点。

### 4.4 `latest:{slotTag}`

key：

```text
redis-gunyu-bisync:<checkpointName>:latest:{slotTag}
```

类型：

```text
HASH
```

字段：

- `version`
- `run_id`
- `syncer_id`
- `unit_seq`
- `start_offset`
- `end_offset`
- `slot`
- `digest`
- `mtime`

例子：

```redis
HSET redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11bb22cc33:latest:{slot-8338-x} \
  version 1 \
  run_id r1 \
  syncer_id syncer-a \
  unit_seq 9 \
  start_offset 10 \
  end_offset 20 \
  slot 8338 \
  digest deadbeef \
  mtime 1710000003000000000
```

含义：

- 对于 `slot=8338`，最新已经确认提交的 replay unit 是 `unit_seq=9`
- 它覆盖的 source offset 范围是 `[10, 20]`
- 恢复时可以把该 slot 的已提交点视为 `offset=20`

`latest` 只在 bisync `sync` 模式中充当 authoritative 恢复依据。

### 4.5 `commit:{slotTag}:<unitSeq>`

key：

```text
redis-gunyu-bisync:<checkpointName>:commit:{slotTag}:<unitSeq>
```

例子：

```text
redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11bb22cc33:commit:{slot-8338-x}:00000000000000000009
```

类型：

```text
HASH
```

字段：

- `version`
- `run_id`
- `syncer_id`
- `unit_seq`
- `start_offset`
- `end_offset`
- `slot`
- `digest`
- `mtime`

例子：

```redis
HSET redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11bb22cc33:commit:{slot-8338-x}:00000000000000000009 \
  version 1 \
  run_id r1 \
  syncer_id syncer-a \
  unit_seq 9 \
  start_offset 10 \
  end_offset 20 \
  slot 8338 \
  digest deadbeef \
  mtime 1710000003000000000
```

这个 key 存储的就是：

- 某一个 replay unit 已经成功提交
- 它对应哪个 source runId
- 它覆盖的 offset 范围
- 它落在哪个 slot
- 它在本 namespace 中的单调序号 `unit_seq`

这是 `pipeline`/`parallel` 模式的 journal record。

注意：

- `commit` 不是“当前全局最新位置”
- `commit` 只是“某个 unit 已经提交”的证据
- 它必须和 `frontier` 联合起来，才能形成 authoritative 恢复点

### 4.6 `index:{slotTag}`

key：

```text
redis-gunyu-bisync:<checkpointName>:index:{slotTag}
```

类型：

```text
ZSET
```

score：

- `unit_seq`

member：

- 对应 `commit` key 的完整名字

例子：

```redis
ZADD redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11bb22cc33:index:{slot-8338-x} \
  9 redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11bb22cc33:commit:{slot-8338-x}:00000000000000000009
```

作用：

- 为 `pipeline`/`parallel` 模式的 commit journal 提供按 `unit_seq` 的有序索引
- 启动恢复时，先从 `index` 找到候选 `commit` key，再去读取对应 hash

`index` 自己不保存恢复点，只保存索引关系。

### 4.7 `rdb:{slotTag}:<unitSeq>`

key 形式：

```text
redis-gunyu-bisync:<checkpointName>:rdb:{slotTag}:<unitSeq>
```

目前状态：

- 代码里定义了这个 key 形式
- parser 也能识别
- 但当前主写路径并不会真正落这个 key

当前 RDB bisync 主路径实际只写：

- `marker`
- 业务命令

不会写独立的 `rdb record`。

因此在理解当前线上恢复行为时，可以把这个 key 看成“预留/兼容解析能力”，而不是当前 checkpoint 主数据的一部分。

<a id="5-bisync-sync启动恢复与运行更新"></a>
## 5. bisync sync：启动恢复与运行更新

`sync` 模式的核心特点：

- 每个 slot 只保留一个 `latest`
- 不保留 commit journal
- 恢复时扫描所有 slot 的 `latest`，从中选出最合适的恢复点

### 5.1 启动恢复

假设 Redis 中有以下数据：

```redis
HSET redis-gunyu-checkpoint-hash r1 redis-gunyu-checkpoint-bisync:aa11
HSET redis-gunyu-checkpoint-bisync:aa11 bisync_mode sync

HSET redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11:latest:{slot-a} \
  version 1 run_id r1 syncer_id syncer-a unit_seq 7 start_offset 81 end_offset 100 slot 100 digest d1 mtime 10

HSET redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11:latest:{slot-b} \
  version 1 run_id r1 syncer_id syncer-a unit_seq 8 start_offset 101 end_offset 120 slot 200 digest d2 mtime 11

HSET redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11:latest:{slot-c} \
  version 1 run_id r1 syncer_id syncer-a unit_seq 6 start_offset 61 end_offset 90 slot 300 digest d3 mtime 9
```

启动恢复流程：

1. source 当前返回 `runId = r1`
2. GunYu 先去 `redis-gunyu-checkpoint-hash` 找到：

   ```text
   r1 -> redis-gunyu-checkpoint-bisync:aa11
   ```

3. 根据 `checkpointName=redis-gunyu-checkpoint-bisync:aa11`，扫描所有 recovery slots 对应的 `latest:{slotTag}`
4. 只保留 `run_id` 匹配 `r1` 的记录
5. 在这些记录中选出：

   - `end_offset` 最大的
   - 若 offset 相同，则 `mtime` 更新的优先

6. 最终上面三条里会选：

   ```text
   latest:{slot-b}
   end_offset = 120
   ```

7. 因此启动恢复点为：

   ```text
   run_id = r1
   offset = 120
   ```

`sync` 模式下，authoritative 恢复点不是 root key 里的 `r1_offset`，而是扫描 `latest` 后选出的最佳记录。

### 5.2 运行更新

假设一个新的 replay unit：

- `unit_seq = 9`
- `slot = 8338`
- `start_offset = 121`
- `end_offset = 140`
- 业务命令只有一条：

  ```redis
  SET foo{slot-8338-x} value
  ```

`sync` 模式会把它包装成一笔真实事务：

```redis
MULTI
SET redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11:marker:{slot-8338-x} \
  '{"version":"1","run_id":"r1","syncer_id":"syncer-a","unit_seq":9,"start_offset":121,"end_offset":140,"slot":8338,"digest":"deadbeef"}' \
  PX 86400000
SET foo{slot-8338-x} value
HSET redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11:latest:{slot-8338-x} \
  version 1 \
  run_id r1 \
  syncer_id syncer-a \
  unit_seq 9 \
  start_offset 121 \
  end_offset 140 \
  slot 8338 \
  digest deadbeef \
  mtime 1710000010000000000
EXEC
```

提交成功后的语义：

- `marker` 告诉反向解析器，这是一笔 mirrored transaction
- 业务命令成功落地
- `latest:{slot-8338-x}` 成为该 slot 的最新 authoritative checkpoint

下一次重启时，如果这条 `latest` 是所有 slot 中 offset 最大的记录，那么恢复起点就会直接选到 `offset=140`

<a id="6-bisync-pipelineparallel启动恢复与运行更新"></a>
## 6. bisync pipeline/parallel：启动恢复与运行更新

`pipeline`/`parallel` 模式的核心特点：

- 提交时记录 `commit journal`
- 用 `index` 建立 journal 索引
- 用 `frontier` 表示“已经连续闭合”的全局恢复前沿
- 恢复时不是简单取最大 `unit_seq`，而是从 `frontier` 后继续向前连续拼接

### 6.1 启动恢复

先看一个简单例子。

Redis 中已有：

```redis
HSET redis-gunyu-checkpoint-hash r1 redis-gunyu-checkpoint-bisync:aa11
HSET redis-gunyu-checkpoint-bisync:aa11 bisync_mode parallel

HSET redis-gunyu-checkpoint-bisync:aa11:frontier \
  version 1 \
  run_id r1 \
  unit_seq 9 \
  end_offset 321 \
  mtime 456

HSET redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11:commit:{slot-1}:00000000000000000010 \
  version 1 \
  run_id r1 \
  syncer_id syncer-a \
  unit_seq 10 \
  start_offset 322 \
  end_offset 400 \
  slot 1 \
  digest d10 \
  mtime 457

ZADD redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11:index:{slot-1} \
  10 redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11:commit:{slot-1}:00000000000000000010
```

启动恢复流程：

1. source 当前返回 `runId = r1`
2. GunYu 从 `redis-gunyu-checkpoint-hash` 找到 `checkpointName=redis-gunyu-checkpoint-bisync:aa11`
3. 读取：

   ```text
   redis-gunyu-checkpoint-bisync:aa11:frontier
   ```

   得到：

   ```text
   frontier.unit_seq = 9
   frontier.end_offset = 321
   ```

4. 计算：

   ```text
   minSeq = frontier.unit_seq + 1 = 10
   ```

5. 扫所有 slot 的 `index:{slotTag}`，执行逻辑等价于：

   ```redis
   ZRANGEBYSCORE <indexKey> 10 +inf
   ```

6. 得到候选 `commit` key
7. 对这些 `commit` key 执行 `HGETALL`
8. 按 `unit_seq` 重建连续前沿

   在这个例子里：

   - 当前 frontier 已到 `seq=9`
   - 恰好存在 `seq=10`
   - 中间没有缺口

9. 因此前沿可推进为：

   ```text
   unit_seq = 10
   end_offset = 400
   ```

10. 最终启动恢复点就是：

    ```text
    run_id = r1
    offset = 400
    ```

再看一个“有洞”的例子。

如果 Redis 中只有：

```text
frontier.unit_seq = 8
commit(seq=10) 存在
commit(seq=9) 不存在
```

那么启动恢复时：

- 不能直接跳到 `seq=10`
- 恢复点必须停在 `frontier(seq=8)` 对应的 offset

原因是：

- `pipeline`/`parallel` 模式只承认“从 frontier 之后连续闭合”的 journal
- 不能因为看见更大的 `unit_seq` 就越过中间缺口

这就是 `frontier + commit journal` 的核心语义。

### 6.2 运行更新

假设新的 replay unit 如下：

- `unit_seq = 9`
- `slot = 8338`
- `start_offset = 121`
- `end_offset = 140`
- 业务命令：

  ```redis
  SET foo{slot-8338-x} value
  ```

`pipeline`/`parallel` 模式会先提交一笔真实事务：

```redis
MULTI
SET redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11:marker:{slot-8338-x} \
  '{"version":"1","run_id":"r1","syncer_id":"syncer-a","unit_seq":9,"start_offset":121,"end_offset":140,"slot":8338,"digest":"deadbeef"}' \
  PX 86400000
SET foo{slot-8338-x} value
HSET redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11:commit:{slot-8338-x}:00000000000000000009 \
  version 1 \
  run_id r1 \
  syncer_id syncer-a \
  unit_seq 9 \
  start_offset 121 \
  end_offset 140 \
  slot 8338 \
  digest deadbeef \
  mtime 1710000010000000000
ZADD redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11:index:{slot-8338-x} \
  9 \
  redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11:commit:{slot-8338-x}:00000000000000000009
EXEC
```

注意，这一步提交成功后：

- `commit` 已经出现
- `index` 已经出现
- 但 `frontier` 不一定立刻推进

原因是：

- `pipeline`/`parallel` 允许多个 unit in-flight
- 真正推进恢复面的动作由 coordinator 串行完成
- coordinator 只有在发现 `frontier.unit_seq + 1` 连续可达时，才会推进前沿

举例：

当前已有：

```text
frontier = seq 8, offset 120
```

这时：

- `unit 10` 比 `unit 9` 更早执行成功

Redis 里会先出现：

```text
commit(seq=10)
index(score=10 -> commitKey10)
```

但 `frontier` 仍然保持：

```text
seq = 8
offset = 120
```

直到 `unit 9` 也完成，coordinator 才会：

1. 把 `seq=9` 和 `seq=10` 一起吸收入连续前沿
2. 更新 `checkpointName:frontier`
3. 删除已经被 frontier 吞并的 `commit` key
4. 对对应的 `index` 做 `ZREM`

更新后的效果可能是：

```redis
HSET redis-gunyu-checkpoint-bisync:aa11:frontier \
  version 1 \
  run_id r1 \
  unit_seq 10 \
  end_offset 160 \
  mtime 1710000011000000000

DEL redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11:commit:{slot-8338-x}:00000000000000000009
DEL redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11:commit:{slot-8338-x}:00000000000000000010
ZREM redis-gunyu-bisync:redis-gunyu-checkpoint-bisync:aa11:index:{slot-8338-x} <commitKey9> <commitKey10>
```

所以 `pipeline`/`parallel` 模式的恢复面是分两层推进的：

1. 提交层：先写 `commit + index`
2. 收敛层：再由 `frontier` 吸收连续部分

## 7. 启动时 checkpoint namespace 的创建与迁移

bisync 启动时，首先要确定当前 runId 对应哪个 `checkpointName`。

流程如下：

1. 用 source 当前 `runId/runId2` 去查 `redis-gunyu-checkpoint-hash`
2. 如果查到了 `checkpointName`

   - 直接复用这个 namespace
   - 再读取 root key 里的 `bisync_mode`

3. 如果没查到

   - 创建新的 `redis-gunyu-checkpoint-bisync:<random>`
   - 写回 `runId -> checkpointName`
   - 给 root key 写入 `bisync_mode`

4. 如果查到旧 namespace，但 mode 与当前期望 mode 不一致

   - 从旧 namespace 中提取 authoritative seed
   - 生成新的 `checkpointName`
   - 在新 namespace 中写入最小恢复状态
   - 把 `checkpoint-hash` 重新指向新 namespace
   - 尝试清理旧 namespace

两个迁移例子：

1. `sync -> pipeline|parallel`

   - 从旧 namespace 扫 `latest`
   - 选出最佳 `latest`
   - 把它转成 seed
   - 在新 namespace 中写 root checkpoint 和 `frontier`

2. `pipeline|parallel -> sync`

   - 先读取旧 `frontier`
   - 再读取 `frontier` 之后的 `commit journal`
   - 重建连续前沿
   - 把前沿转成 seed
   - 在新 namespace 中写 root checkpoint 和一个 `latest`

所以：

- `checkpoint-hash` 是入口索引
- `checkpointName` 是稳定 namespace root
- `latest` 和 `frontier + commit journal` 才是 mode-specific authoritative 状态

## 8. 删除与 GC

checkpoint 相关删除主要有三类。

### 8.1 删除普通 checkpoint 字段

普通 `DelCheckpoint` 会在对应 `checkpointName` hash 中删除：

- `<runId>_runid`
- `<runId>_offset`
- `<runId>_version`
- `<runId>_mtime`

这个删除逻辑主要是给非 bisync 或 shared root checkpoint 字段使用。

### 8.2 周期性 stale checkpoint GC

GC 的总体思路：

1. 先收集当前 input 侧仍然活着的 `runId`
2. 扫描 `redis-gunyu-checkpoint-hash`
3. 对每个 `runId -> checkpointName`

   - 如果 runId 仍存在，只删除过期副本，但尽量保留最新一份
   - 如果 runId 已不存在，且这个 checkpoint 的全部副本都删空了，再删除 `checkpoint-hash` 里的映射

这个 GC 主要围绕普通 checkpoint root 做，不负责通用扫描所有 bisync 派生 key。

### 8.3 bisync namespace 迁移后的显式清理

当 bisync namespace 发生 mode 迁移时，旧 namespace 一旦从 `checkpoint-hash` 脱钩，常规 stale GC 就未必还能通过 `runId` 找到它。

因此代码会做一次显式 best-effort cleanup，删除：

- root key：`checkpointName`
- frontier key：`checkpointName:frontier`
- per-slot `marker`
- per-slot `latest`
- per-slot `index`
- `pipeline`/`parallel` 模式下索引里引用到的所有 `commit` key

这一步保证 mode 迁移后不会遗留失联的旧 bisync namespace。

## 9. 关键结论

可以把当前实现总结成下面三句话。

1. 非 bisync

   - checkpoint 主体就是 `checkpointName` hash
   - authoritative 恢复点就是 `<runId>_offset`

2. bisync `sync`

   - root key 负责 namespace 和 mode
   - authoritative 恢复点来自各 slot 的 `latest`

3. bisync `pipeline`/`parallel`

   - `commit` 只表示单个 unit 已提交
   - `index` 只表示 journal 索引
   - `frontier` 才表示当前已经连续闭合的全局恢复前沿
   - authoritative 恢复点来自 `frontier + commit journal` 的连续重建结果

如果只看一个 checkpoint key，很容易误解恢复语义。

当前代码的正确理解方式是：

- 先区分非 bisync / bisync
- 在 bisync 内再区分 `sync` / `pipeline` / `parallel`
- 最后再判断某个 key 是索引、抑制面控制数据，还是 authoritative 恢复面数据
