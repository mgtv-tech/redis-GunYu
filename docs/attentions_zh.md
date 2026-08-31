# 注意事项

- [注意事项](#注意事项)
  - [redis版本](#redis版本)
    - [源端和目的端](#源端和目的端)
  - [命令兼容性](#命令兼容性)
  - [全量同步](#全量同步)
  - [Sentinel切换一致性](#sentinel切换一致性)
  - [源端redis集群扩容与缩容导致数据不一致](#源端redis集群扩容与缩容导致数据不一致)




## redis版本

redis-GunYu支持从4.0到8.x的redis版本。

当前已经支持 Redis 8 的 RDB v13，以及 Redis 8 cluster RDB 中的 `SLOT_INFO` 元数据 opcode。

### 源端和目的端

源和目标redis集群的版本最好一致，主要由于：
1. RDB回放：restore命令有版本要求；当然，如果restore失败，`redis-GunYu`会尝试通过redis命令的方式对RDB数据进行回放。
2. 扩容和slot迁移：数据迁移时，会发送restore-asking命令，也有版本要求

redis兼容性，请见[测试文档](test_zh.md#版本兼容测试)


## 命令兼容性

不支持的命令如下
- flush*
- bgsave， save
- cluster


**EVALSHA ： redis 5以下的版本，若源redis未开启AOF，则EVALSHA命令可能会有问题**

如果源redis没有开启AOF，且目标redis没有缓存lua脚本，则源redis执行的EVALSHA命令，无法在目标集群执行。   
为了避免这种问题，请开启源redis的AOF或者升级redis。



## 全量同步

`redisGunYu`全量同步RDB到目标端前，不会对目标端数据进行清理，而是直接回放RDB数据，这样就会存在目标端数据比源端多。

所以，如果要保证数据一致性，则请手动进行强制flushdb和全量同步，请参考[强制flushdb API](API_zh.md#强制全量同步)


原因：   
- 如果源和目标redis的slots可能不是对应的(cross slots)，如果只对其中一个节点进行全量同步，则无法对目标端redis执行flushdb命令
- 如果执行flushdb，将RDB数据同步到目标端期间，目标端redis数据的某些keys可能不存在，造成不一致
- RDB回放可能失败，导致目标端缺少数据

## Sentinel切换一致性

Redis GunYu 会定期重新解析 Sentinel 拓扑。当选中的源节点或目标 master 改变时，
受影响的 syncer 会被重建，并继续使用现有复制 ID、本地 channel 和 checkpoint
恢复流程；无法增量恢复时会退化为全量同步。

Sentinel failover 建立在 Redis 异步复制之上。旧 master 已接受但尚未复制到新
master 的写入可能丢失。系统仍是最终一致/弱一致，不提供零 RPO、exactly-once
或强一致切换。对非幂等命令，应在切换后同时校验业务值和 checkpoint 状态。



## 源端redis集群扩容与缩容导致数据不一致


迁移源端redis的SLOT期间，redis会给SLOT迁移目的节点发送restore key命令，然后在原节点执行删除keys的命令，然后同步时，这是两个pipeline，所以，会存在异步执行的问题，同步到目标端，则可能先执行restore，再执行删除操作，那么这样，key就在目标端不存在了。


**解决方案**

- 1. 强制进行一次全量同步，例如slot 10从节点A迁移到节点B，那么迁移完后，将B节点执行一次全量同步，参考[强制全量同步API](API_zh.md#强制全量同步)
- 2. 修改redis扩容、缩容、slot迁移脚本
