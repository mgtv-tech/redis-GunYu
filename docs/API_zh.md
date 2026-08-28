# 接口


API只在sync命令(实时同步)下有效。


- [接口](#接口)
  - [进程](#进程)
    - [进程退出](#进程退出)
  - [同步](#同步)
    - [启动时暂停与运行期控制](#启动时暂停与运行期控制)
    - [重启同步流程](#重启同步流程)
    - [暂停同步](#暂停同步)
    - [恢复同步](#恢复同步)
    - [同步状态信息](#同步状态信息)
    - [同步配置信息](#同步配置信息)
    - [强制全量同步](#强制全量同步)
    - [转移同步节点](#转移同步节点)
  - [回收本地缓存](#回收本地缓存)
  - [可观测性](#可观测性)
    - [普罗米修斯指标接口](#普罗米修斯指标接口)



支持HTTP接口来进行相关运维操作，如指标采集，停止进程，全量同步等等。


## 进程

### 进程退出

DELETE http://http_server:port/ 

```
curl -XDELETE http://http_server:port/
```

或者使用信号
```
Kill $PID
```

服务默认会以优雅的方式停止进程，所以会等待所有资源回收才会退出，在配置文件中配置 `server.gracefullStopTimeout` 来配置优雅等待超时时间（默认5秒）。


## 同步

### 启动时暂停与运行期控制

当 GunYu 进程需要先加入拓扑发现和 HA 选举、确认解析出的输入节点和选举角色，
或在受控切换前由运维人员确认时，可使用 `server.initialPaused: true`。启用后，
GunYu 在开始从源 Redis 读取或向目标 Redis 写入前保持暂停。

```yaml
server:
  listen: 127.0.0.1:18001
  initialPaused: true
```

按正常方式启动 GunYu，然后等待所有预期 input 在 `/syncer/status` 中显示
`State: "pause"`：

```bash
curl http://127.0.0.1:18001/syncer/status
```

此时 Redis 复制的输入和输出管线尚未启动；拓扑发现以及 HA 模式下的 leader
选举仍会运行。要启动本 GunYu 进程管理的全部管线，恢复所有 input：

```bash
curl -XPOST 'http://127.0.0.1:18001/syncer/resume?inputs=all'
```

如需只恢复一个 input，请先从 `/syncer/status` 获取其准确的 `Input` 值，再传给
`inputs`：

```bash
curl -XPOST 'http://127.0.0.1:18001/syncer/resume?inputs=127.0.0.1:16379'
```

在将切换视为生效前，请确认受影响管线的 `State` 已变为 `"run"`。`resume`
操作是幂等的，因此自动化程序可以安全重试。

已运行的指定管线可通过 `pause` 暂停：

```bash
curl -XPOST 'http://127.0.0.1:18001/syncer/pause?inputs=all'
```

当 `/syncer/restart` 重建同步器时，所选 input 的运行期状态会保留：暂停的
input 仍保持暂停，运行中的 input 会继续运行。在 HA 模式下，需要在每个允许
复制的 GunYu 进程上恢复相应 input；follower 等待成为 leader 时也可以处于
暂停或运行状态。之后发生 leader 切换时会保留该进程本地的状态。

`initialPaused` 只影响新初始化的 input。省略此项或设为 `false` 时，GunYu
保持默认行为并自动启动同步。停止进程应使用 `DELETE /` 接口；这不会代表后续
启动时执行 resume，新的进程应通过 `/syncer/status` 确认当前状态。

### 重启同步流程

POST http://http_server:port/syncer/restart
```
curl -XPOST http://http_server:port/syncer/restart
```


### 暂停同步
```
curl -XPOST 'http://server:port/syncer/pause?inputs=inputIP&flushdb=yes'
```
URL，查询参数：
- inputs : 需要全量同步的源端redis IPs，如果所有源端都全量同步，则写成 inputs=all。如果多个源端IP，则用逗号分隔

### 恢复同步
```
curl -XPOST 'http://server:port/syncer/resume?inputs=inputIP&flushdb=yes'
```
URL，查询参数：
- inputs : 需要全量同步的源端redis IPs，如果所有源端都全量同步，则写成 inputs=all。如果多个源端IP，则用逗号分隔

启用 `server.initialPaused` 后，应等待预期同步器均以 `pause` 状态可见，再使用 `inputs=all` 启动本进程的全部同步管线。重复恢复操作是幂等的。




### 同步状态信息

GET http://http_server:port/syncer/status
```
curl http://http_server:port/syncer/status
```
返回结果
```
[
    {
        "Input": "127.0.0.1:16311",   // 源端redis节点
        "Role": "leader",             // leader或者follower，代表是此节点负责这个redis实例的复制
        "Transaction":true,           // 是否处于事务模式
        "State": "run"                // 运行状态
    },
    {
        "Input": "127.0.0.1:16302",
        "Role": "leader",
        "Transaction":true,   
        "State": "run"
    },
    {
        "Input": "127.0.0.1:16310",
        "Role": "leader",
        "Transaction":true,   
        "State": "run"
    }
]
```


### 同步配置信息

查询当前配置

默认是yaml格式
```
GET http://http_server:port/syncer/config
```
或者json格式
```
GET http://http_server:port/syncer/config?format=json
```




### 强制全量同步
```
curl -XPOST 'http://http_server:port/syncer/fullsync?inputs=inputs&flushdb=yes' 
```
URL，查询参数：
- inputs : 需要全量同步的源端redis IP和端口，如果所有源端都全量同步，则写成 inputs=all。如果多个源端IP+端口，则用逗号分隔。具体IP+端口可以通过`http://http_server:port/syncer/status`接口获取
- flushdb ： 全量同步前，是否执行flushdb

flushdb=yes时，如果是部分源端进行全量同步，则要保证源和目的redis的slots能够一一对应，否则请全量同步所有的源端(inputs=all)


```
# 强制全量同步127.0.0.1:16302和127.0.0.1:16310两个源redis节点；且清空相对应的目的redis节点的数据(执行flushdb)
curl -XPOST 'http://http_server:port/syncer/fullsync?inputs=127.0.0.1:16302,127.0.0.1:16310&flushdb=yes'
```



### 转移同步节点

当`redisGunYu`是集群模式部署时，若要将某个`redisGunYu`节点负责的同步权转移到其他`redisGunYu`节点，则可以使用`handover` API。
```
curl -XPOST 'http://http_server:port/syncer/handover?inputs=inputs' 
```
URL，查询参数：
- inputs : 需要全量同步的源端redis IP和端口，如果所有源端都全量同步，则写成 inputs=all。如果多个源端IP+端口，则用逗号分隔。具体IP+端口可以通过`http://http_server:port/syncer/status`接口获取


当某个`redisGunYu`节点需要下线时，可以使用这个API来将此`redisGunYu`节点的同步权转移到其他节点后，再下线。


## 回收本地缓存

GET http://http_server:port/storage/gc
```
curl http://http_server:port/storage/gc
```


## 可观测性
### 普罗米修斯指标接口

GET http://http_server:port/prometheus
