# RDB 命令


- [RDB 命令](#rdb-命令)
  - [将RDB文件回放到redis实例或redis集群中](#将rdb文件回放到redis实例或redis集群中)
  - [解析RDB文件，输出到标准输出或者文件中](#解析rdb文件输出到标准输出或者文件中)


## 将RDB文件回放到redis实例或redis集群中

此功能是解析RDB文件，然后将数据回放到正在运行的redis中，可以对RDB文件进行过滤。


通过配置文件运行，可以参考config/rdb_load.yaml配置文件。   
将`/tmp/test.rdb`文件导入到`127.0.0.1:6379,127.0.0.1:6479`的redis集群中，且忽略掉DB 1，忽略test_ignore前缀的keys。
```
./redisGunYu -cmd=rdb -conf=config/rdb_load.yaml
```


通过命令行参数运行
```
./redisGunYu -cmd=rdb -rdb.action=load -rdb.rdbPath=/tmp/test.rdb -rdb.load.redis.addresses=127.0.0.1:6379,127.0.0.1:6479 -rdb.load.redis.type=cluster -rdb.load.filter.dbBlacklist=1 -rdb.load.filter.keyFilter.prefixKeyBlacklist=test_ignore
```



**配置**

配置文件分为：
- action : 执行的子命令
- rdbPath : RDB文件路径
- load : RDB文件导入相关配置
  - redis : redis相关配置，可以参考[同步配置redis配置](sync_configuration_zh.md#redis配置)
  - replay : 回放相关配置，可以参考[同步配置replay配置](sync_configuration_zh.md#replay配置)
  - filter : 过滤相关配置


以下是一个简单的演示配置文件
```
action: load
rdbPath: /tmp/test.rdb
load:
  redis:
    addresses: [127.0.0.1:6379,127.0.0.1:6479]
    type: cluster
  filter:
    dbBlacklist: 1
    keyFilter:
      prefixKeyBlacklist: test_ignore
```

### Redis Modules RDB 边界

RDB load 对 Redis 模块 keyspace 数据类型采用 opaque `RESTORE` 回放，不解析模块内部格式。目标 Redis 必须加载兼容模块，并保持 `replay.replayRdbEnableRestore: true`。

已用 Redis Stack 验证 RedisJSON / RedisBloom 的 keyspace module object 可以恢复。RediSearch index 等 `MODULE_AUX` 全局模块元数据当前不支持。默认 `replay.moduleAuxPolicy: fail` 会在发现这类元数据时停止 RDB 回放且不写 checkpoint；只有确认被跳过的元数据会由增量模块命令或外部流程重建时，才应设置 `replay.moduleAuxPolicy: skip`。


## 解析RDB文件，输出到标准输出或者文件中

解析/tmp/dump.rdb文件，并输出到标准输出
```
./redisGunYu -cmd=rdb -rdb.action=print -rdb.rdbPath=/tmp/dump.rdb
```


**配置**

配置文件分为：
- action : 执行的子命令
- rdbPath : RDB文件路径
- print : 打印RDB文件内容的相关参数
  - output : 输出文件，默认是标准输出
  - noLogKey : 不输出key，默认是false
  - noLogValue : 不输出value，默认是false

一般使用命令行参数指定配置比较方便，如下：
```
./redisGunYu -cmd=rdb -rdb.action=print -rdb.rdbPath=/tmp/dump.rdb -rdb.print.output=/tmp/rdb.log -rdb.print.noLogKey=true -rdb.print.noLogValue=true
```
