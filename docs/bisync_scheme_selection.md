# Cluster 双向同步 Marker 方案设计

- [Cluster 双向同步 Marker 方案设计](#cluster-双向同步-marker-方案设计)
  - [1. 背景](#1-背景)
  - [2. 目标与非目标](#2-目标与非目标)
    - [2.1 目标](#21-目标)
    - [2.2 非目标](#22-非目标)
  - [3. 当前实现现状](#3-当前实现现状)
    - [3.1 恢复语义仍然是节点级流](#31-恢复语义仍然是节点级流)
    - [3.2 bisync 当前已落地真实事务主路径](#32-bisync-当前已落地真实事务主路径)
  - [4. 设计评估维度](#4-设计评估维度)
  - [5. 方案一：Marker 按 Slot 粒度，使用真实事务包装](#5-方案一marker-按-slot-粒度使用真实事务包装)
    - [5.1 方案描述](#51-方案描述)
    - [5.2 优势](#52-优势)
      - [5.2.1 边界最清晰](#521-边界最清晰)
      - [5.2.2 反向解析最简单](#522-反向解析最简单)
      - [5.2.3 多命令事务天然可支持](#523-多命令事务天然可支持)
    - [5.3 问题与风险](#53-问题与风险)
      - [5.3.1 最大问题是恢复，不是发送](#531-最大问题是恢复不是发送)
      - [5.3.2 重启后 checkpoint 还原有两种做法](#532-重启后-checkpoint-还原有两种做法)
        - [5.3.2.1 做法 A：checkpoint 也写入 MULTI/EXEC 事务](#5321-做法-acheckpoint-也写入-multiexec-事务)
        - [5.3.2.2 做法 B：checkpoint 不进事务，由定时任务写固定 key](#5322-做法-bcheckpoint-不进事务由定时任务写固定-key)
        - [5.3.2.3 小结](#5323-小结)
      - [5.3.3 需要新的“全局连续 frontier”机制](#533-需要新的全局连续-frontier机制)
      - [5.3.4 cross-slot unit 天然不成立](#534-cross-slot-unit-天然不成立)
      - [5.3.5 实现复杂度最高](#535-实现复杂度最高)
    - [5.4 结论](#54-结论)
  - [6. 方案二：Marker 按 Redis 节点粒度，沿用当前伪事务批量方式](#6-方案二marker-按-redis-节点粒度沿用当前伪事务批量方式)
    - [6.1 方案描述](#61-方案描述)
      - [6.1.1 为什么说它是节点粒度](#611-为什么说它是节点粒度)
    - [6.2 优势](#62-优势)
      - [6.2.1 恢复语义最接近当前系统](#621-恢复语义最接近当前系统)
      - [6.2.2 实现复杂度最低](#622-实现复杂度最低)
      - [6.2.3 性能更容易接受](#623-性能更容易接受)
    - [6.3 核心问题](#63-核心问题)
      - [6.3.1 它不具备真实事务边界](#631-它不具备真实事务边界)
      - [6.3.2 可能发生部分成功](#632-可能发生部分成功)
      - [6.3.3 无法把“没有交叉执行”当作 correctness 前提](#633-无法把没有交叉执行当作-correctness-前提)
    - [6.4 子方案 2A：一个 Marker 后面跟多条命令](#64-子方案-2a一个-marker-后面跟多条命令)
      - [6.4.1 表面优势](#641-表面优势)
      - [6.4.2 实际问题](#642-实际问题)
      - [6.4.3 结论](#643-结论)
    - [6.5 子方案 2B：一个 Marker 后面只跟一条命令](#65-子方案-2b一个-marker-后面只跟一条命令)
      - [6.5.1 方案描述](#651-方案描述)
      - [6.5.2 优势](#652-优势)
      - [6.5.3 问题](#653-问题)
      - [6.5.4 如何优化](#654-如何优化)
      - [6.5.5 结论](#655-结论)
  - [7. 方案三：Marker 后跟一条命令还是多条命令](#7-方案三marker-后跟一条命令还是多条命令)
    - [7.1 Marker 后跟多条命令](#71-marker-后跟多条命令)
    - [7.2 Marker 后跟一条命令](#72-marker-后跟一条命令)
  - [8. 方案对比](#8-方案对比)
  - [9. 推荐方案](#9-推荐方案)
    - [9.1 当前选择：方案一](#91-当前选择方案一)
      - [选择理由](#选择理由)
        - [理由一：事务边界必须对 Redis 可见](#理由一事务边界必须对-redis-可见)
        - [理由二：2B 的弱关联在 cluster 下不可证明](#理由二2b-的弱关联在-cluster-下不可证明)
        - [理由三：恢复复杂度高，但仍然是必须解决的问题](#理由三恢复复杂度高但仍然是必须解决的问题)
    - [9.2 不推荐的方案](#92-不推荐的方案)
      - [强烈不推荐方案二A](#强烈不推荐方案二a)
      - [明确不再采用方案二B](#明确不再采用方案二b)
  - [10. 后续演进建议](#10-后续演进建议)
    - [优先方向](#优先方向)
    - [不再保留的思路](#不再保留的思路)
  - [11. 最终结论](#11-最终结论)




## 1. 背景

`redis-GunYu` 当前的实时同步链路，本质上是：
- 输入端：对源 Redis 节点执行 `PSYNC`
- 通道端：本地落盘缓存
- 输出端：将解析出的 RDB/AOF 命令回放到目标 Redis

当前系统的恢复轴是：
- **每个 source redis 节点一条线性 replication stream**
- **恢复点由 source runId + source offset 决定**

双向同步的核心问题不是“如何把命令再写一遍”，而是：
- 如何避免 A -> B 的镜像写再次被 B -> A 回流
- 如何在 cluster 场景下保证恢复语义不被破坏

本文聚焦 cluster 输出端的 Marker 设计，分析不同方案的优劣势，并给出推荐方案。

说明：

- 本文主要是 cluster 方案设计文档
- 当前代码实现除了 cluster 之外，也已经支持 standalone 双向同步的真实事务路径
- 关于当前落地实现，请以 [docs/bisync_scheme1_impl.md](../docs/bisync_scheme1_impl.md) 为准

## 2. 目标与非目标

### 2.1 目标

- 为 cluster 双向同步设计一版可落地的 Marker 方案
- 明确每种方案的边界条件、故障模式与恢复代价
- 给出最终采用的工程方案

### 2.2 非目标

- 不讨论业务冲突解决，例如 LWW、CRDT、向量时钟
- 不尝试把当前系统直接升级为真正的多主强一致复制
- 不讨论 standalone 真实事务版本的完整实现细节

## 3. 当前实现现状

### 3.1 恢复语义仍然是节点级流

当前 `redis-GunYu` 的恢复逻辑依赖 source `runId + offset`。

这意味着：

- 源端的复制流是**节点级线性流**
- checkpoint 本质上描述的是“这条 source stream 已安全回放到哪里”
- bisync 只是把恢复元数据挂在稳定 `checkpointName` 命名空间下，而不是改变恢复轴本身
- `runId/replid` 当前只负责查询 `redis-gunyu-checkpoint-hash`，不直接充当 bisync namespace

### 3.2 bisync 当前已落地真实事务主路径

当前 bisync 主路径已经不是历史上的 cluster pseudo batch，而是会把 replay unit 发送成目标 Redis 可见的真实 `MULTI/EXEC`。

当前实现特征包括：

- 事务内写入 `marker + business commands + latest/commit(+index)`
- `pipeline=false` 时以 slot-local `latest` 推进恢复面
- `pipeline=true` 时以 `commit record + commit index + checkpointName:frontier` 推进恢复面
- bisync namespace 直接由稳定 `checkpointName` 决定，不再依赖 target 当前 slot 视图或 source 地址
- cluster 下恢复默认扫描全 `16384` slot，因此 target reshard / failover 后仍能命中旧 metadata
- 目标拓扑变化通过 `MOVED/ASK` 重定向按整个 replay unit 重试提交

需要区分的是：

- bisync 主路径已经具备“真实事务 Marker”语义
- 非 bisync / legacy 普通回放路径仍然保留原有 batch 发送模型
- 当前落地细节仍以 [docs/bisync_scheme1_impl.md](../docs/bisync_scheme1_impl.md) 为准

## 4. 设计评估维度

所有方案都从下面 6 个维度评估：

1. **回环抑制正确性**
   Marker 能否稳定识别镜像流量
2. **恢复语义**
   重启后能否用清晰、单调的 checkpoint 恢复
3. **实现复杂度**
   对现有架构侵入有多大
4. **性能代价**
   是否明显放大写入、降低吞吐
5. **对非幂等命令的安全性**
   比如 `INCR`、`LPUSH`、`XADD`
6. **cluster 兼容性**
   对 slot、node、拓扑变更的适应能力

## 5. 方案一：Marker 按 Slot 粒度，使用真实事务包装

### 5.1 方案描述

核心思想：
- Marker 不再是节点级，而是跟着 replay unit 走
- 每个 replay unit 必须能映射到一个明确的 slot 事务单元
- 在目标 Redis 侧真实执行：

```redis
MULTI
SET <slot-marker-key> <marker-value> PX <ttl>
...business commands...
HSET <checkpoint-key> ...
EXEC
```

Marker 的语义是：
- 该事务中的业务命令来自对端镜像回放
- 反向链路看到这一整段事务后，应整批抑制



### 5.2 优势

#### 5.2.1 边界最清晰

这是唯一一个 Marker 语义和 Redis 自身事务边界一致的方案。

好处：
- Marker 覆盖范围明确
- 不需要猜 marker 后面到底跟几条业务命令
- 不需要担心普通客户端命令插入同一个事务边界中

#### 5.2.2 反向解析最简单

反向链路只要按 `MULTI ... EXEC` 解析：

- 如果事务第一条控制命令是合法 Marker
- 且来源是对端 cluster
- 则整批丢弃

#### 5.2.3 多命令事务天然可支持

如果这批业务命令本来就可以在 cluster 上真实事务提交，那么：

- 一个 Marker 后面跟多条命令是合理的
- 不需要拆成每条命令一个 Marker

### 5.3 问题与风险

#### 5.3.1 最大问题是恢复，不是发送

source 复制流是节点级线性 offset。

但这个方案把目标端提交单元变成了 slot 粒度事务。于是会立刻遇到恢复问题：

- slot A 的事务已提交到 offset 100
- slot B 的事务只提交到 offset 95

重启时该从哪个 offset 恢复？

如果取最大：
- 会漏掉未提交完成的 slot B 事务

如果取最小：
- 会重放已提交的 slot A 事务

对于非幂等命令，这种重放不可接受。

#### 5.3.2 重启后 checkpoint 还原有两种做法

结合当前 `redis-GunYu` 已有的两种发送模式：

- **同步模式**：写入 Redis，等待 reply，再继续
- **异步模式**：持续写入 Redis，同时异步回收 reply

方案一在“checkpoint 怎么恢复”上，实际上有两种常见做法。

##### 5.3.2.1 做法 A：checkpoint 也写入 MULTI/EXEC 事务

写法类似：

```redis
MULTI
SET <slot-marker-key> <marker-value> PX <ttl>
...business commands...
HSET <slot-checkpoint-key> offset <end-offset> ...
EXEC
```

它的优势是：

- 业务命令和 checkpoint 具有同一个提交边界
- `EXEC` 成功则两者都成功
- `EXEC` 失败则两者都失败
- 不会出现“业务命令已落地，但 checkpoint 还没写”的窗口

但它是否足够，取决于发送模型。

**在同步模式下：**

- 如果严格保证“同一条 source stream 任意时刻只有一个 replay unit 在飞”
- 即前一个 unit 拿到 `EXEC` reply 之后，才发送后一个 unit

那么：

- 已成功提交的 unit 天然构成一个连续前缀
- 各 slot 上虽然各自维护 checkpoint，但它们不会跳跃越过前面的未提交 unit
- 重启时扫描所有 slot 的 checkpoint，取**最大 offset**，是成立的

原因是：

- offset 100 不可能先于 offset 96~99 被提交
- 所以最大 offset 代表“全局已经连续提交到哪里”

这意味着：

- **方案一 + 同步模式 + checkpoint 随事务提交**
- 可以不引入复杂 frontier 机制
- 代价是吞吐会明显下降，因为 cluster 双向回放退化成全局串行提交

**在异步模式下：**

- 不同 slot 的 unit 可能同时在飞
- 后面的 offset 可能先提交成功，前面的 offset 可能还没提交

例如：

- slot A 的 unit 已提交到 offset 100
- slot B 的 unit 只提交到 offset 95
- 而 offset 96~99 对应的 unit 仍在飞或失败未提交

此时：

- 扫描所有 slot，取最大值 100，会漏掉 96~99
- 取最小值 95，会重放 slot A 上已成功的 96~100

所以在异步模式下，这个做法本身**不够**：

- 事务内 checkpoint 只能表达“某个 slot 的局部最新提交进度”
- 不能直接作为全局 authoritative restart checkpoint

##### 5.3.2.2 做法 B：checkpoint 不进事务，由定时任务写固定 key

另一种思路是：

- 业务命令仍按方案一走真实事务
- 但 checkpoint 不写进事务
- 而是由独立定时任务把“当前观察到的最新 offset”写入一个固定 key

它的优势是：

- 实现简单
- 不需要在每个 slot 上维护 checkpoint key
- 重启时读取固定 key 较快
- 热点集中，管理简单

但它有一个结构性问题：

- checkpoint 和业务提交不在同一个事务边界

因此会天然存在 crash window。

例如：

- `EXEC` 已成功
- 但定时 checkpoint 还没刷到固定 key
- 此时进程崩溃

那么重启后：

- 只会读到旧 offset
- 已成功提交的业务 unit 可能被重放

对非幂等命令，这种重复执行不可接受。

而在异步模式下，它的问题更大：

- 定时任务看到的“最新 offset”不一定代表连续提交前缀
- 它可能只是“目前已经收到的某个较大 offset 的成功回包”
- 仍然不能推出前面所有更小 offset 都已经安全提交

所以这个做法最多只能充当：

- **checkpoint snapshot / cache**

而不能作为：

- **唯一 authoritative checkpoint**

##### 5.3.2.3 小结

两种做法的本质差异不是“key 写在哪里”，而是：

- checkpoint 是否和业务提交共用同一个事务边界
- 当前发送模型是否允许多个 offset 对应的 unit 同时在飞

结论如下：

- **做法 A 在同步模式下可成立**
- **做法 A 在异步模式下仍然不够**
- **做法 B 无论同步还是异步，都更适合作为 snapshot，而不适合作为强语义恢复点**

换句话说：

- 如果方案一实施时愿意接受**全局串行提交**，那么可以选择：
  - checkpoint 跟业务命令一起写入事务
  - 重启时扫描所有 slot checkpoint，取最大 offset 恢复
- 如果方案一仍希望保留**异步并发能力**，那么仅靠这两种 checkpoint 做法都不够，仍然需要额外的全局恢复协调机制

#### 5.3.3 需要新的“全局连续 frontier”机制

checkpoint机制
- 如果是“方案一 + 做法 A + 同步模式”，slot-local checkpoint 可能已经足够
- 如果是“方案一 + 做法 A + 异步模式”，那 需要frontier 机制就
- 做法 B 则因为 checkpoint 不和事务共边界，最多只能做 snapshot/cache，不是这里要表达的正确解

要把该方案真正做对，必须新增一套恢复协调机制，例如：

- 每个 slot/unit 的提交进度单独记录
- 再维护一个“所有 unit 都已连续提交”的全局 offset frontier

这个 frontier 不能靠简单的最大值、最小值或 mtime 拼出来。

否则恢复语义不成立。

#### 5.3.4 cross-slot unit 天然不成立

如果一个 replay unit 涉及多个 slot：
- 就不能放进一个真实 cluster 事务
- 必须拆分

而一旦拆分，又会进一步加剧“节点级 source offset vs slot 级提交”的恢复矛盾。

#### 5.3.5 实现复杂度最高

这个方案不仅要改发送路径，还要改：
- replay unit 切分方式
- checkpoint 元数据模型
- 恢复算法
- 失败重放策略

它不是 Marker 小改，而是恢复架构升级。

### 5.4 结论

这是当前应采用的 cluster Marker 方案。

主要原因：
- 只有它把 marker、业务命令、提交边界绑定在 Redis 可见的真实事务里
- 只有它能把回环抑制建立在“事务级镜像写”上，而不是 marker 与业务命令相邻的弱假设上
- 恢复复杂度虽然更高，但这是 correctness 必须支付的代价，不能再用 2B 的弱关联去回避






## 6. 方案二：Marker 按 Redis 节点粒度，沿用当前伪事务批量方式

### 6.1 方案描述

核心思想：
- 不改变当前节点级恢复轴
- 继续沿用现有 cluster pseudo batch 路径
- Marker 作为一种“镜像命令提示信号”插入到输出流中

本方案内部又分为两种子方案：
- 2A：一个 Marker 后面跟多条命令
- 2B：一个 Marker 后面只跟一条命令

#### 6.1.1 为什么说它是节点粒度

因为：
- authoritative checkpoint 仍然是一条 source pipeline 一个
- 重启恢复仍按 source node 的 runId + offset
- 不引入 per-slot checkpoint lane

这是它最大的现实优势。

### 6.2 优势

#### 6.2.1 恢复语义最接近当前系统

因为不拆 source stream：
- checkpoint 仍然只有一个主恢复点
- 重启逻辑不需要聚合多个 slot checkpoint
- 与当前 PSYNC 语义天然一致

#### 6.2.2 实现复杂度最低

这个方案主要修改：
- AOF parser
- 输出命令组装
- Marker 编码/解码

不强迫重写整个恢复链路。

#### 6.2.3 性能更容易接受

因为不要求所有 replay unit 都走真实事务：
- 吞吐损失相对可控
- 更接近当前 cluster 回放方式

### 6.3 核心问题

#### 6.3.1 它不具备真实事务边界

由于 Redis 侧看不到真正的 `MULTI/EXEC`：
- Marker 与业务命令之间只是“约定上的相邻关系”
- 不是 Redis 语义保证的原子单元

这会带来一个根本问题：
- Marker 只能做“弱关联”
- 不能做“强事务包裹”

#### 6.3.2 可能发生部分成功

当前 pseudo batch 路径允许出现：
- Marker 成功
- 部分业务命令成功
- 后续业务命令失败
- checkpoint 未及时前进

这意味着：
- 一个 Marker 覆盖越多命令，恢复和判定越困难
- 对非幂等命令越危险

#### 6.3.3 无法把“没有交叉执行”当作 correctness 前提

即便从网络行为上看，一次 flush 往往会连续被 Redis 处理，也不能把它当设计前提。

原因是：
- 当前方案没有 Redis 可见边界
- 一旦连接中断、失败重试、部分回包异常，逻辑边界会立刻丢失
- correctness 不能建立在“通常不会插队”这种实现经验上

所以：
- 必须按“可能交叉、可能部分成功”来设计
- 不能假设多条命令天然属于同一个 Marker 单元

### 6.4 子方案 2A：一个 Marker 后面跟多条命令

#### 6.4.1 表面优势

- Marker 数量少
- 写放大小
- 性能更好

#### 6.4.2 实际问题

在 pseudo batch 模式下，这个子方案问题最大。

原因：
- Marker 覆盖范围只能靠“发送端约定”
- 反向链路很难可靠判断哪几条命令属于这个 Marker
- 一旦中途部分成功，恢复后无法精确知道哪些命令已落地

具体风险：
- Marker 成功，但只成功了一半业务命令
- checkpoint 记录的是 batch 尾 offset，但中间命令并非全都可靠提交
- 非幂等命令重放时会发生重复副作用

#### 6.4.3 结论

**不推荐。**

这个子方案看似节省命令数，但会把问题从“发送效率”转移成“恢复不可解释”。

### 6.5 子方案 2B：一个 Marker 后面只跟一条命令

#### 6.5.1 方案描述

发送序列严格收敛成：

```redis
SET <shard-syncer-marker-key> <marker-value> PX <ttl>
<one business command>
```

其中 Marker 明确描述：
- 这是哪条 source offset 对应的镜像命令
- 这条业务命令的摘要指纹是什么

这里有一个额外前提需要显式满足：
- `marker` 和对应业务命令必须落到同一个 target cluster shard / node
- 否则反向 parser 会在某个节点先看到 Marker，但永远看不到对应业务命令

因此 key 设计不能只用一个全局固定 key，而要改成：
- **每个 target shard 一把确定性 marker key**
- **每个 peer syncer pipeline 在该 shard 上各自独立**

也就是：
- Marker 粒度 = `target shard + syncer pipeline`

key 生成要求：
- 不带随机性，拓扑不变时重启后生成结果必须一致
- 不能只依赖 source 拓扑
- 应基于 target shard 的 slot ranges 生成稳定签名，再选出一个稳定落在该 shard slots 内的 key

反向 parser 只在下面条件同时满足时抑制：
- 先看到合法 Marker
- 后面紧跟一条合法业务命令
- 该业务命令与 Marker 中的指纹完全匹配

parser 状态也不能只保留一个全局 pending marker，而应改成：
- `pending markers by syncerID`

这样当多个对端 syncer pipeline 的镜像写同时进入同一个 target shard 时，pending 状态不会相互覆盖。

#### 6.5.2 优势

- 关联范围最小
- 反向识别逻辑最明确
- 恢复语义仍是节点级单 checkpoint
- 即便失败，最多污染一条命令的归属，不会扩大成整批误判
- 通过“按 target shard 选 marker key”，可以兼容源/目标 cluster 拓扑不一致

#### 6.5.3 问题

- 写放大显著
- 吞吐会下降
- 仍然不是真事务
- 对非幂等命令仍然敏感
- 发送侧必须先确定业务命令的 target shard，再选择对应 marker key
- parser 需要维护 `syncerID -> pending marker` 的多路状态，而不是单 pending

更关键的问题是：即便把关联范围缩成“一条 marker 对一条业务命令”，它仍然无法解决 pseudo batch 的交叉执行问题。

在 cluster 下，发送序列只是 client 侧约定的相邻关系，而不是 Redis 侧可见事务边界。因此可能出现：

- marker 已写入目标节点
- 对应业务命令因为路由、重试、连接切换、批次拆分、回包时序等原因，没有与该 marker 保持严格相邻
- Redis 在目标节点上把这批 pseudo transaction 写入与业务侧真实写入交叉执行

一旦发生这种交叉：

- parser 看到的是“marker 到了，但下一条可见业务命令不是它要匹配的命令”
- 原本应该被抑制的镜像写会变成“marker 无法匹配”
- 这些未匹配镜像写继续被当作业务写回流，形成回环

对幂等命令，这会放大同步噪音；对 `INCR`、`LPUSH`、`XADD` 等非幂等命令，则会直接放大副作用，风险不可接受。

例如：
- Marker 成功
- 业务命令成功
- checkpoint 尚未更新

如果重启从旧 offset 重放，仍可能重复执行这条业务命令。

所以这个方案本质上仍是：
- best-effort 回环抑制
- 不是强事务一致性

另外，在 cluster 下如果 marker key 仍是单个固定 key，会出现一个结构性问题：
- `marker` 路由到节点 A
- 业务命令路由到节点 B
- 反向 parser 在 A 上只能看到 marker，看不到对应业务命令
- 如果后面继续有新的 marker 进入 A，就会产生“旧 marker 未匹配，新 marker 又到来”的假异常

因此，per-shard deterministic marker key 不是性能优化，而是 correctness 前提。

#### 6.5.4 如何优化

如果采用这个子方案，建议增加以下约束：
- Marker 只标记紧随其后的 1 条命令
- Marker 中包含命令摘要，如 `cmd + key hash + args hash + offset`
- cluster 下 marker key 必须按 `target shard + syncer pipeline` 生成，并且确定性稳定
- 发送侧先按业务命令路由 key 计算 target shard，再注入该 shard 的 marker key
- parser 使用 `pending markers by syncerID` 维护待匹配状态
- parser 只在“单个 pending marker 与单条业务命令严格指纹匹配”时做抑制
- 遇到异常时默认走 best-effort：
- 非法 marker / TTL / decode 失败：忽略 marker，记录日志和指标
- 同一 syncer 出现新 marker 覆盖旧 pending：告警并替换旧 pending
- pending 超过窗口未匹配：告警并丢弃 pending
- 指纹冲突或多 pending 同时命中：告警并丢弃相关 pending，不中断同步链路

这样做的原因是：
- cluster pseudo batch 本身不是强事务
- fail-stop 只能提高可见性，不能根治 partial success
- 在生产链路中，更合理的是让异常 marker 降级为“本次未抑制成功”，而不是让整个 syncer 退出

需要明确边界：
- best-effort 不是“最多只会回环一次”的严格保证
- 如果 marker 连续失效，非幂等命令仍然可能多次回环
- 因此必须配套日志、指标和告警，便于识别异常链路

#### 6.5.5 结论

**不可取。**

即便补上：

- marker key 按 `target shard + syncer pipeline` 确定性生成
- parser 按 `syncerID` 维度维护 pending
- 各种 best-effort 告警与降级

它仍然没有解决最核心的问题：

- marker 与业务命令之间没有 Redis 可见事务边界
- pseudo transaction 写入可能与业务写入交叉执行
- 弱匹配失败后会把镜像写重新放回业务路径

因此，2B 不是“精度差一点但还能接受”的折中方案，而是 correctness 上不可证明的方案。它不能作为当前实现，也不能作为方案一失败时的 fallback。

## 7. 方案三：Marker 后跟一条命令还是多条命令

这个问题本质上不是独立方案，而是方案二的核心分叉。

### 7.1 Marker 后跟多条命令

适用条件：
- 只有在“真实事务边界存在”的前提下才合理

在 cluster pseudo batch 路径下的问题：
- 没有强边界
- 命令归属不清
- 恢复困难
- 非幂等命令风险放大

结论：
- **真实事务下可行**
- **伪事务下不推荐**

### 7.2 Marker 后跟一条命令

适用条件：
- 当没有真实事务边界时，必须把 Marker 关联范围收缩到最小

优点：
- 逻辑最可解释
- 反向匹配最简单
- 故障面最小

缺点：
- 写放大高
- 吞吐下降

结论：
- 在 pseudo batch 前提下，**即便缩成一条 marker 对一条命令，也仍然不可取**

## 8. 方案对比

| 维度 | 方案一：slot 粒度真实事务 | 方案二A：节点粒度 + 一个 Marker 多条命令 | 方案二B：节点粒度 + 一个 Marker 一条命令 |
| :- | :- | :- | :- |
| 回环抑制正确性 | 高 | 低 | 中 |
| 恢复语义清晰度 | 中，需要新 frontier，但语义可证明 | 低 | 低 |
| 实现复杂度 | 最高 | 低 | 中 |
| 性能 | 中 | 高 | 低到中 |
| 非幂等命令安全性 | 高 | 最差 | 低 |
| 与当前架构契合度 | 中 | 中 | 表面高，实则 correctness 不成立 |
| 当前推荐度 | 高 | 低 | 不可取 |

## 9. 推荐方案

### 9.1 当前选择：方案一

推荐：

- **方案一**
- 即：**Marker 按 slot 粒度，与业务命令一起进入真实 `MULTI/EXEC`**
- **反向链路按事务边界整批识别并抑制镜像写**
- **恢复语义通过 `latest` / `commit record + frontier` 建模，而不是靠 pending marker 弱匹配**

#### 选择理由

##### 理由一：事务边界必须对 Redis 可见

双向同步的核心不是“尽量给命令打上 marker”，而是要让目标端能够明确区分：

- 哪些写入属于镜像回放
- 这些写入的提交边界在哪里
- 反向链路应该抑制哪一个完整单元

这件事只有在 marker、业务命令、checkpoint/journal 与 `EXEC` 绑定时才成立。否则 parser 看到的永远只是“猜测上的相邻关系”。

##### 理由二：2B 的弱关联在 cluster 下不可证明

2B 的核心假设是：

- marker 与其业务命令可以靠相邻关系稳定匹配
- 即便没有真实事务，也能把不可靠性收敛到“一条命令”

这个假设在 cluster pseudo batch 下不成立。因为 Redis 只看到一串普通命令，并不知道“这条 marker 应该和哪条业务命令绑定”。一旦出现交叉执行、部分成功、重试、连接切换或回包乱序，marker 就可能匹配不到原始业务命令，镜像写就会重新进入业务路径，形成回环。

尤其对非幂等命令，这不是“偶尔重复一次”的小问题，而是会持续放大副作用的结构性问题。

##### 理由三：恢复复杂度高，但仍然是必须解决的问题

方案一的代价确实更高：

- 要把 source stream 切成 replay unit
- 要保证 unit 单 slot、同事务提交
- 要补齐 authoritative 恢复所需的 frontier / journal

但这些不是“额外追求完美”的附加项，而是 cluster 双向同步 correctness 的组成部分。既然 2B 已经被证明会在压力和交叉执行场景下失效，就不能再因为实现便宜而继续采用。

### 9.2 不推荐的方案

#### 强烈不推荐方案二A

原因：

- 在 pseudo batch 下，一个 Marker 覆盖多条命令的边界不可验证
- 部分成功和恢复重放问题最难解释

#### 明确不再采用方案二B

原因：

- 它仍然建立在 marker 与业务命令“相邻即可匹配”的弱前提上
- 它无法防止 pseudo transaction 写入与业务写入在 Redis 侧交叉执行
- 一旦交叉，marker 就可能长期匹配不到原始业务命令，镜像写重新进入业务路径
- 对非幂等命令，这会造成不可接受的重复副作用

因此，2B 不是保底方案，也不是灰度方案，而是明确不再采用的历史思路。

## 10. 后续演进建议

### 优先方向

继续围绕方案一补强：

- namespace-local slot hint / active slot index，但只能作为 cache；缺失或不完整时必须回退全 `16384` slot 扫描
- target reshard / failover / source runId 漂移相关的故障注入与集成测试覆盖
- 保持 `bisyncEnabled` 作为唯一显式 bisync 开关，避免回归到隐式推导
- cluster 下仍走 legacy fallback 的全局 opcode / 特殊路径继续 bisync 化

### 不再保留的思路

后续设计和实现中不再保留：

- 2A：一个 marker 覆盖多条伪事务命令
- 2B：一个 marker 对应一条命令的 pending-marker 弱匹配
- 任何建立在“通常不会交叉执行”“大概率连续到达”上的 best-effort correctness 假设

## 11. 最终结论

在当前 `redis-GunYu` 架构下，cluster 双向 Marker 的核心取舍是：

- **事务边界必须对 Redis 可见**
- **恢复复杂度不能靠弱关联 marker 规避**

因此：

- **当前采用方案：方案一**
- **明确放弃方案：方案二A、方案二B**

即：

- Marker 与业务命令按 slot 粒度进入真实事务
- 反向抑制基于事务边界，而不是 pending marker 弱匹配
- 恢复通过 `latest` / `frontier + commit record` 这类 durable 元数据完成
- 不再接受 2B 那种“marker 无法稳定匹配时让镜像写重新进入业务路径”的方案
