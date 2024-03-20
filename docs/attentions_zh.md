# 注意事项

- [注意事项](#注意事项)
  - [redis版本](#redis版本)
    - [源端和目的端](#源端和目的端)
  - [命令兼容性](#命令兼容性)
  - [全量同步](#全量同步)
  - [redis集群扩容与缩容](#redis集群扩容与缩容)




## redis版本

redis-GunYu支持从4.0到7.2的redis版本。

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


## 全量同步

`redisGunYu`全量同步RDB到目标端前，不会对目标端数据进行清理，而是直接回放RDB数据，这样就会存在目标端数据比源端多。

所以，如果要保证数据一致性，则请手动进行强制flushdb和全量同步，请参考[强制flushdb API](API_zh.md#强制全量同步)


原因：   
- 如果源和目标redis的slots可能不是对应的(cross slots)，如果只对其中一个节点进行全量同步，则无法对目标端redis执行flushdb命令
- 如果执行flushdb，将RDB数据同步到目标端期间，目标端redis数据的某些keys可能不存在，造成不一致
- RDB回放可能失败，导致目标端缺少数据



## redis集群扩容与缩容


迁移SLOT期间，redis会给SLOT迁移目的节点发送restore key命令，然后在原节点执行删除keys的命令，然后同步时，这是两个pipeline，所以，会存在异步执行的问题，同步到目标端，则可能先执行restore，再执行删除操作，那么这样，key就在目标端不存在了。

**解决方案**

- 1. 强制进行一次全量同步，例如slot 10从节点A迁移到节点B，那么迁移完后，将B节点执行一次全量同步，参考[强制全量同步API](API_zh.md#强制全量同步)
- 2. 修改redis扩容、缩容、slot迁移脚本

