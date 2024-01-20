# 配置

现阶段使用yaml文件来配置参数，后期会增加命令行和环境变量方式。


## 配置文件


配置文件分为以下几个配置组：
- id ： redis-GunYu的唯一ID，类型为字符串
- input ：输入端的配置
- output ： 输出端的配置
- channel ： 本地缓存配置
- filter ： 过滤策略配置
- cluster ： 集群模式配置
- log ： 日志配置
- server ： 服务器相关配置


**通用配置**

redis配置
- addresses ： redis地址， 数组
- userName ： redis用户名
- password ： redis密码
- type ： redis类型
  - standalone ： 根据addresses里的地址来同步
  - cluster ： 
- clusterOptions
  - replayTransaction ： 是否尝试使用事务进行同步，默认开启


**input**

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


**output**

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
- targetDbMap ： 同步db的映射结构
- batchCmdCount ： 批量同步命令的数量，将batchCmdCount数量的命令打包同步，默认100
- batchTickerMs ： 批量同步命令的等待时间，最多等待batchTickerMs毫秒再进行打包同步，默认20，代表20ms
- batchBufferSize ： 批量同步命令的缓冲大小，当打包缓冲区的大小超过batchBufferSize，则进行同步，默认64KB。batchCmdCount、batchTickerMs、batchBufferSize三者是或关系，只要满足一个，就进行同步。
- replayRdbParallel ： 用几个线程来回放RDB，默认为CPU数量
- updateCheckpointTickerMs ： 默认1000，1s


**channel**

channel配置
- storer ： rdb和aof存储区
  - dirPath ： 存储目录
  - maxSize ： 存储最大空间，单位字节
  - logSize ： 每个aof文件大小
  - flush ： 同步aof文件到磁盘的策略，默认是auto
    - duration ： 每个多久刷新一次
    - everyWrite ： 每次写入命令到aof后，进行同步
    - dirtySize ： 当写入aof文件数据超过dirtySize，则进行同步
    - auto ： 由操作系统自己决定
- verifyCrc : 默认false
- staleCheckpointDuration ： 多久以前的快照视为过期快照，默认12小时


**filter**


**cluster**

集群模式配置
- groupName ： 组名，同一个组的redis-GunYu会进行选举一个leader作为同步的主库
- metaEtcd ： etcd配置
  - endpoints ： etcd节点地址
  - username ： 用户名
  - password ： 密码
- leaseTimeout ： leader租期时间，如果在leaseTimeout时间内，leader没有续租，则表示leader过期了，会重新发起选举；默认10秒，值范围为[3s, 600s]
- leaseRenewInterval ： leader发起租期时间间隔，默认3.33秒，一般选为leaseTimeout的1/3，值范围为[1s, 200s]
- replica ： 主从相关配置
  - listen ： 监听的地址，如 "0.0.0.0:8081"
  - listenPeer ： 主库的通信地址，从库根据此地址和主库同步数据，如"172.27.83.13:8081"



**log**

日志配置
- level ： 级别，默认info，级别有debug, info, warn, error, panic, fatal
- handler ： 
  - file ： 日志输出到文件
    - fileName ： 文件名
    - maxSize ： 文件大小，单位为MiB
    - maxBackups ： 最大日志文件数量
    - maxAge ： 日志保留天数
  - stdout ： 默认输出到标准输出


**server**

服务器配置
- httpBind ： http服务器监听的网络接口，默认是本机所有网络接口
- httpPort ： http服务器监听的端口，默认18000
- checkRedisTypologyTicker ： 检查redis cluster拓扑的时间周期，单位秒，默认30秒
- gracefullStopTimeout ： 优雅退出超时时间，默认5秒


## 配置文件示例

```
id: 1 
server:
  httpPort: 8801   
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

# redis-GunYu高可用相关配置，如果不需要支持高可用，则可以忽略
cluster:
  groupName: redis1
  leaseTimeout: 3s
  metaEtcd: 
    endpoints:
      - 127.0.0.1:2379
  replica:
    listen: "0.0.0.0:8088"
    listenPeer: "127.0.0.1:8088"
```


