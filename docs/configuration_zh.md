# 配置

- [配置](#配置)
  - [配置文件](#配置文件)
    - [通用配置](#通用配置)
    - [输入端](#输入端)
    - [输出端](#输出端)
    - [缓存区](#缓存区)
    - [过滤](#过滤)
    - [集群](#集群)
    - [日志](#日志)
    - [服务器](#服务器)
  - [配置文件示例](#配置文件示例)
    - [最小配置文件](#最小配置文件)
    - [较完善配置](#较完善配置)
  - [命令行参数](#命令行参数)



`redisGunYu`支持以配置文件启动，或者以命令参数方式启动。



## 配置文件


配置文件分为以下几个配置组：
- input ：输入端redis（源端）的配置
- output ： 输出端redis（目标端）的配置
- channel ： 本地缓存配置
- cluster ： 集群模式配置
- log ： 日志配置
- server ： 服务器相关配置
- filter ： 过滤策略配置


### 通用配置

redis配置
- addresses ： redis地址， 数组。如果redis是cluster部署的，则`addresses`最好配置多于1个节点的IP地址，避免1个节点故障而无法联系redis集群。
- userName ： redis用户名
- password ： redis密码
- type ： redis类型
  - standalone ： 根据addresses里的地址来同步
  - cluster ： 
- clusterOptions
  - replayTransaction ： 是否尝试使用事务（伪事务，不是基于multi/exec，而是将redis命令打包一次性发送到redis端执行）进行同步，默认开启


### 输入端

由于现在仅支持从redis同步，所以输入端输出端都只有redis端的配置。

input配置如下
- redis ： redis配置
- rdbParallel ： 同一时刻进行rdb的限制数，默认没有限制
- mode ： 
  - static ： 仅同步redis配置里的节点
  - dynamic ：如果redis是cluster，则同步redis cluster所有节点
- syncFrom ： 
  - prefer_slave ： 优先从从库同步，如果从库不可用，则使用主库同步
  - master ： 使用主库同步
  - slave ： 使用从库同步


### 输出端

output配置如下：
- redis ： redis配置
- resumeFromBreakPoint ： 是否开启断点续传，默认开启
- keyExists ： output中key存在，如何处理
  - replace ： 替换，默认值
  - ignore ： 忽略
  - error ： 报错，停止同步
- keyExistsLog ： 配合keyExists使用，默认关闭
  - true ： 如果keyExists是replace，则替换key时，打印info日志；如果keyiExists是ignore，则替换key时，打印warning日志
  - false ： 关闭keyExists日志
- functionExists ： 如何回放函数字段，参考`FUNCTION RESTORE`命令参数
  - flush ： 
  - replace ： 
- maxProtoBulkLen ： 协议最大的缓存区大小，参考redis配置`proto-max-bulk-len`，默认是512MiB
- targetDb ： 选择同步到output的db，默认-1，表示根据input的db进行对应同步
- batchCmdCount ： 批量同步命令的数量，将batchCmdCount数量的命令打包同步，默认100
- batchTickerMs ： 批量同步命令的等待时间，最多等待batchTickerMs毫秒再进行打包同步，默认10ms
- batchBufferSize ： 批量同步命令的缓冲大小，当打包缓冲区的大小超过batchBufferSize，则进行同步，默认64KB。batchCmdCount、batchTickerMs、batchBufferSize三者是或关系，只要满足一个，就进行同步。
- replayRdbParallel ： 用几个线程来回放RDB，默认为CPU数量
- updateCheckpointTickerMs ： 默认1秒
- keepaliveTicker ： 默认3秒，保持心跳时间间隔


> 同步延迟主要取决于`batchCmdCount`和`batchTickerMs`，工具会将命令打包发送到目标端，只要两个配置中的一个满足则即可


### 缓存区

配置
- storer ： rdb和aof存储区
  - dirPath ： 存储目录，默认使用`/tmp/redis-gunyu`
  - maxSize ： 存储最大空间，单位字节，默认50GiB
  - logSize ： 每个aof文件大小，默认100MiB
  - flush ： 同步aof文件到磁盘的策略，默认是auto
    - duration ： 每个多久刷新一次
    - everyWrite ： 每次写入命令到aof后，进行同步
    - dirtySize ： 当写入aof文件数据超过dirtySize，则进行同步
    - auto ： 由操作系统自己决定
- verifyCrc : 默认false
- staleCheckpointDuration ： 多久以前的快照视为过期快照，默认12小时


### 过滤

- commandBlacklist :  命令黑名单，数组结构，忽略掉这些命令
- keyFilter: 对key进行过滤
  - prefixKeyBlacklist : 前缀key黑名单
  - prefixKeyWhitelist : 前缀key白名单


如下配置，不同步del命令，也不同步redisGunYu开头的key
```
filter:
  commandBlacklist:
    - del
  keyFilter:
    prefixKeyBlacklist: 
      - redisGunYu
```


### 集群

集群模式配置
- groupName ： 集群名，此名字在etcd集群中作为集群名使用，所以请确保唯一性
- metaEtcd ： etcd配置
  - endpoints ： etcd节点地址
  - username ： 用户名
  - password ： 密码
- leaseTimeout ： leader租期时间，如果在leaseTimeout时间内，leader没有续租，则表示leader过期了，会重新发起选举；默认10秒，值范围为[3s, 600s]
- leaseRenewInterval ： leader发起租期时间间隔，默认3.33秒，一般选为leaseTimeout的1/3，值范围为[1s, 200s]


如下配置：
```
cluster:
  groupName: redisA
  leaseTimeout: 9s
  metaEtcd: 
    endpoints:
      - 127.0.0.1:2379
```


### 日志

日志配置
- level ： 级别，默认info，级别有debug, info, warn, error, panic, fatal
- handler ： 
  - file ： 日志输出到文件
    - fileName ： 文件名
    - maxSize ： 文件大小，单位为MiB
    - maxBackups ： 最大日志文件数量
    - maxAge ： 日志保留天数
  - stdout ： 默认输出到标准输出
- withCaller : bool值，日志是否包含源码文件名，默认false
- withFunc : bool值，日志是否包含函数调用者，默认false
- withModuleName : bool值，日志是否包含模块名，默认true



### 服务器

服务器配置
- listen : 监听地址，默认"127.0.0.1:18001"
- listenPeer: 和其他`redis-GunYu`进程通信用途，IP:Port，默认和listen一样。注意不要写成127.0.0.1
- metricRoutePath : prometheus的http路径，默认是 "/prometheus"
- checkRedisTypologyTicker ： 检查redis cluster拓扑的时间周期，默认30秒，可以用1s, 1h，1ms等字符串
- gracefullStopTimeout ： 优雅退出超时时间，默认5秒



## 配置文件示例


### 最小配置文件

```
input:
  redis:
    addresses: [127.0.0.1:10001, 127.0.0.1:10002]   
    type: cluster
output:
  redis:
    addresses: [127.0.0.1:20001]
    type: standalone
```



### 较完善配置
```
server:
  listen: 0.0.0.0:18001 
  listenPeer: 10.220.14.15:18001  # 局域网地址
input:
  redis:
    addresses: [127.0.0.1:6300]
    type: cluster
  rdbParallel: 4
  mode: dynamic
  syncFrom: prefer_slave
channel:
  storer:
    dirPath: /tmp/redisgunyu-cluster
    maxSize: 209715200
    logSize: 20971520
  staleCheckpointDuration: 10m
output:
  redis:
    addresses: [127.0.0.1:6310]
    type: cluster
  resumeFromBreakPoint: true
log:
  level: info
  handler:
    stdout: true
  withModuleName: false

# 集群模式需要配置下面cluster配置，如果没有此配置，则是单实例模式
cluster:
  groupName: redis1
  leaseTimeout: 3s
  metaEtcd: 
    endpoints:
      - 127.0.0.1:2379
```




## 命令行参数

我们可以使用命令行参数的方式启动redisGunYu
```
redisGunYu --sync.input.redis.addresses=127.0.0.1:6379 --sync.output.redis.addresses=127.0.0.1:16379
```

参数名都以`--sync.`作为前缀名，后面则以配置的字段名，用`.`连接起来；数组以`,`进行分隔符。     

如源端redis地址，配置文件如下，
```
input:
  redis:
    addresses: [127.0.0.1:6379, 127.0.0.2:6379]
```

则命令行名为`--sync.input.redis.addresses=127.0.0.1:6379,127.0.0.2:6379`

可以通过`redisGunYu -h`来查看都有哪些参数。

