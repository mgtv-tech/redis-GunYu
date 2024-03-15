# 测试

- [测试](#测试)
  - [Redis相关测试](#redis相关测试)
    - [RDB测试](#rdb测试)
    - [AOF测试](#aof测试)
    - [命令测试](#命令测试)
    - [版本兼容测试](#版本兼容测试)
  - [接口测试](#接口测试)
  - [服务测试](#服务测试)
  - [一致性测试](#一致性测试)
    - [离线测试](#离线测试)
    - [在线环境测试](#在线环境测试)



## Redis相关测试

### RDB测试

|  数据结构  |  RDB类型  |  测试结果  |
| :- | :- | :- |
|  string  |  string  |  通过  |
|  list |  RDBTypeListZiplist  |  通过  |
|  list | RDBTypeList  |  通过  |
|  list | RDBTypeQuicklist  |  通过  |
|  list | RDBTypeQuicklist2  |  通过  |
|  set  | RDBTypeSet  |  通过  |
|  set  | RDBTypeSetIntset  |  通过  |
|  set  | RDBTypeSetListpack  |  通过  |
|  zset  | RDBTypeZSet  |  通过，redis2.0版本数据结构，无须测试  |
|  zset  | RDBTypeZSet2  |  通过  |
|  zset  | RDBTypeZSetZiplist  |  通过  |
|  zset  | RDBTypeZSetListpack  |  通过  |
|  hash  | RDBTypeHashZiplist  |  通过  |
|  hash  | RDBTypeHashZipmap  |  通过  |
|  hash  | RDBTypeHash  |  通过  |
|  hash  | RDBTypeHashListpack  |  通过  |
|  stream  | RDBTypeStreamListPacks  |  通过  |
|  stream  | RDBTypeStreamListPacks2  |  通过  |
|  stream  | RDBTypeStreamListPacks3  |  通过  |
|  function  | RDBFunction2  |  通过  |
|  aux  | expire  |  通过  |
|  aux  | select  |  通过  |
|  非restore模式  | 对RDB数据结构进行拆分，通过命令回放  |  通过  |
|  文件损坏  | 文件损坏检测  |  通过  |
|  多DB  | 多个redis DB  |  通过  |



### AOF测试
|  类别  | 测试case |  测试结果  | 
| :- | :- | :- |
|  文件损坏  | 检测损坏aof | 通过  |
|  数据结构测试 | string |   通过  |
|  数据结构测试 | list |   通过  |
|  数据结构测试 | hash  |   通过  |
|  数据结构测试 | set  |   通过  |
|  数据结构测试 | zset  |   通过  |
|  数据结构测试 | stream  |   通过  |


### 命令测试
|  命令类别  | 测试命令 |  测试结果  | 
| :- | :- | :- |
|  常规命令  | COPY, MOVE, RENAME*, RESTORE | 通过  |
|  hash | HSET, HDEL, HINCRBY*, HSETNX |   通过  |
|  list | BLMOVE, BLMPOP, BL*, BR*, LINSERT, LMOVE, LMPOP, LPOP*, LPUSH*, LREM, LSET, LTRIM, RPOP*, RPUSH*  |   通过  |
|  pub/sub |  PUBLISH, PSUBSCRIBE, SPUBLISH, SSUBSCRIBE,  |   通过  |
| scipt  | FUNCTION LOAD, FUNCTION DELETE, FUNCTION FLUSH, SCRIPT LOAD, SCRIPT FLUSH  |   通过  |
| set  |  SADD, SMOVE, SPOP, SREM,  |   通过  |
| zset |  ZADD, ZINCRBY, ZMPOP, ZPOP*  |   通过  |
| stream  | XADD, XACK, XCLAIM, XDEL, XGROUP*, XTRIM  |   通过  |
| string  | SET*, DEL, EXPIRE*, APPEND, DECR, INCR*, MSET*, PSETEX,   |   通过  |
| 事务 | MULTI, EXEC  |   通过  |


### 版本兼容测试

测试RDB和AOF在不同版本之间兼容性；RDB回放测试RESTORE命令回放和非RESTORE命令回放两种。

| 源端版本 | 目标端版本 |  测试结果  | 备注 |
| :- | :- | :- |  :- | 
| 4.0  | 5.0  | 通过  |   |
| 4.0  | 6.0  | 通过  |   |
| 4.0  | 7.0  | 通过  |   |
| 5.0  | 6.0  | 通过  |   |
| 5.0  | 7.0  | 通过  |   |
| 6.0  | 7.0  | 通过  |   |
| 7.0  | 6.0  | 通过  | redis7以下不支持function命令，工具忽略同步此数据结构  |
| 7.0  | 5.0  | 通过  | redis7以下不支持function命令，工具忽略同步此数据结构  |
| 7.0  | 4.0  | 通过  | redis7以下不支持function命令，工具忽略同步此数据结构  |
| 6.0  | 5.0  | 通过  |   |
| 6.0  | 4.0  | 通过  |   |
| 5.0  | 4.0  | 通过  |   |





## 接口测试
|  类别  | 用例 | 接口 |  测试结果  | 
| :- | :- | :- | :- |
| 进程 | 退出进程 | curl -XDELETE http://server:port/  |   通过  |
| 存储  | 主动触发垃圾回收  | curl http://server:port/storage/gc  |   通过  |
| 同步  | 重启同步流程  | curl -XPOST http://server:port/syncer/restart  |   通过  |
| 同步  | 剔除leader权限  | curl -XPOST http://server:port/syncer/handover  |   通过  |
| 同步  | 强制全量同步  | curl -XPOST 'http://server:port/syncer/fullsync?inputs=inputIP&flushdb=yes' -v  |   通过  |
| 同步  | 同步状态  | curl http://server:port/syncer/status  |   通过  |
| 同步  | 暂停同步  | curl -XPOST http://server:port/syncer/pause  |   通过  |
| 同步  | 恢复同步  | curl -XPOST http://server:port/syncer/resume  |   通过  |



## 服务测试

|  类别  | 测试项 | 用例 |  测试结果  | 备注 |
| :- | :- | :- | :- | :- |
| 高可用 | 主从自动切换 |  |   通过  |   |
| 高可用 | 移动leadership |  |   通过  |   |
| 高可用 | leader续租失败 |  |   通过  |   |
| 高可用 | campaign失败 |  |   通过  |   |
| 进程退出 | 优雅退出 |  |   通过  |   |
| 进程退出 | 信号退出 |  |   通过  |   |
| 进程退出 | 接口退出 |  |   通过  |   |
| 集群 | 两边都是cluster集群 | 根据不同的选节点策略获取源redis节点：master, slave, preferSlave等等 |   通过  |   |
| 集群 | 单实例同步到cluster |  |   通过  |   |
| 集群 | cluster同步到单实例 |  |   通过  |   |
| 集群 | 拓扑定期检查 | 检测到变更，更改同步策略 |   通过  |   |
| 缓存 | 垃圾回收 | 达到限制，回收最老的日志，以降低到限制以下 |   通过  |   |
| 缓存 | 一致性检测 | 检测损坏文件 |   通过  |   |
| 缓存 | 刷盘策略 |  |   通过  |   |
| 目的端redis | 拓扑变更 | 事务模式：接收到MOVE和ASK错误，切换为非事务模式|   通过  |   |
| 目的端redis | 拓扑变更 | 事务模式：定时检测拓扑变更，如变更则尝试切换为非事务模式 |   通过  |   |
| 目的端redis | 拓扑变更 | 非事务模式：定期检测事务变更，若变更，尝试切换为事务模式 |   通过  |   |
| 目的端redis | 故障 | 主库故障 |   通过  |   |
| 目的端redis | 手动failover |  |   通过  |   |
| 目的端redis | slot迁移 |  |   通过  |   |
| 源端redis | 选节点 | 根据配置master, slave, prefer_slave选择从这些角色节点同步 |   通过  |   |
| 源端redis | Redis 主从切换 |  |   通过  |   |
| 源端redis | 添加节点 |  |   通过  |   |
| 源端redis | SLOT变更，事务切换 |  |   通过  |   |
| 源端redis | SLOT处于迁移状态 |  |   通过  |   |
| 源端redis | 故障 | 主库故障 |   通过  | 等待cluster选举出主库继续同步  |
| 源端redis | 故障 | 从库故障 |   通过  | 等待cluster将节点标记为fail  |
| 资源 | CPU | CPU占比稳定 |   通过  |   |
| 资源 | 内存 | 没有内存泄漏 |   通过  |   |
| 资源 | 并发安全 | 检测数据并发安全 |   通过  |   |
| 可观测性 | prometheus指标 | 各个指标正确性 |   通过  |   |
| 可观测性 | 日志 | 日志各配置有效性 |   通过  |   |
| 可观测性 | 日志 | 关键日志是否有打印 |   通过  |   |


## 一致性测试

###  离线测试


**测试目的**

测试源端redis与目标端redis的数据是否一致。


**测试环境**
- 机器：16cores， 32GB内存
- 数据结构：key(set/del)，set(sadd, spop)，list(lpush, lpop)，incr， hash(hset, hdel)
- QPS : 4500
- Redis ： 主（3主3从），从（3主1从）
- 数据量：26万keys

|  同步类型  | case | 测试方法 |  测试结果  |  描述 | 
| :- | :- | :- | :- | :- |
| 事务 | 源端主从切换 | Cluster failover force, Redis-full-check 分别比较源端目标端  |   通过  | 切换10次+，数据一致  |
| 事务 | 目标端主从切换 | Cluster failover force, Redis-full-check分别比较源端目标端  |   通过  | 切换10次+，数据一致  |
| 非事务 | 源端主从切换 | Cluster failover force, Redis-full-check分别比较源端目标端  |   通过  | 切换10次+，数据一致  |
| 非事务 | 目标端主从切换 | Cluster failover force, Redis-full-check分别比较源端目标端  |   通过  | 数据少量不一致(取决于QPS)，异步复制导致。由于偏移更新在不同的节点，偏移更新成功，命令执行在主库，主从切换后，由于redis异步复制，导致丢失。属于预期内  |
| slot迁移 | 源端迁移 | 对源端进行扩容、缩容或迁移slots  |   **不通过**  | 迁移slots的数据有不一致情况  |
| slot迁移 | 目标端迁移 | 对目标端进行扩容、缩容或迁移slots  |   通过  |   |


> 对源端进行slot迁移，此slot数据有不一致的情况，我们计划在下个版本解决



### 在线环境测试

持续运行中，暂无问题
