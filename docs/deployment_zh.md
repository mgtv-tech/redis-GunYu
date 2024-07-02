# 部署


- [部署](#部署)
  - [部署redis-GunYu](#部署redis-gunyu)
    - [单节点部署](#单节点部署)
    - [集群部署](#集群部署)
  - [可观测性](#可观测性)
    - [监控](#监控)
      - [监控指标报警](#监控指标报警)
    - [日志](#日志)



## 部署redis-GunYu

### 单节点部署

单节点部署，配置文件不用配置`cluster`相关的配置。直接启动即可
```
redisGunYu -conf config.yaml
```


### 集群部署



修改配置文件，增加`cluster`相关配置，参考[集群配置](configuration_zh.md#集群)，或其[demo配置](configuration_zh.md#较完善配置)


> 线上部署建议使用集群部署方式




## 可观测性

### 监控

`redis-GunYu`支持prometheus指标收集，`GET http://http_server:port/prometheus`。  



**部署步骤**
- 编辑prometheus配置文件，如prometheus.yaml，添加`redis-GunYu`的job，并附加上cluster标签。
```
scrape_configs:
  - job_name: "redisGunYu"
    metrics_path: /prometheus
    static_configs:
      # clusterA集群的redis-GunYu，以cluster的label进行区分
      - targets: ["localhost:19000","localhost:18000"]
        labels:
          cluster: 'clusterA'
      # clusterB集群的redis-GunYu，
      - targets: ["localhost:28000"]
        labels:
          cluster: 'clusterB'
```
- 将deploy目录下的[`grafana_zh.json`](../deploy/grafana_zh.json)导入到grafana。(grafana页面右上角加号，选择`import dashboard`)


**注意**

如果要查看时间维度的同步延迟，需要在配置的`input`中指定同步延迟测试`syncDelayTestKey`的key，要避免与redis数据的key冲突。
```
input:
  syncDelayTestKey: redis-GunYu-syncDelay-testKey
```
> `redis-GunYu`会定时写入此key数据到源端redis，然后同步到目标端时，计算此时间间隔。


请参考[同步延迟配置](configuration_zh.md#输出端)



#### 监控指标报警

下面是监控指标的一些报警，仅供参考，阈值按照用户需求自己调整

**进程**

- 进程故障
```
up{cluster="$cluster"} == 0
```

- 进程内存占用大于1GB
```
process_resident_memory_bytes{cluster="$cluster"} > 1073741824
```

- 并发数大于2000
```
go_goroutines{cluster="$cluster"} > 2000
```


**同步次数报警**

- 10分钟，全量同步超过1次
```
(redisGunYu_input_sync_type{cluster="$cluster", sync_type="full"} OR on() vector(0)) - (redisGunYu_input_sync_type{cluster="$cluster", sync_type="full"} offset 10m OR on() vector(0)) > 1
```

- 10分钟，同步流程启动次数超过2次
```
(redisGunYu_input_sync_type{cluster="$cluster"} OR on() vector(0)) - (redisGunYu_input_sync_type{cluster="$cluster"} offset 10m OR on() vector(0)) > 2
```


**同步延迟报警**

- 延迟大于1秒
```
# redisGunYu_output_sync_delay 单位是纳秒
max(redisGunYu_output_sync_delay{cluster="$cluster"}) > 1000000000
```

- 延迟大于10MB
```
max(abs(sum(redisGunYu_input_offset{cluster="$cluster"}) by(input) - sum(redisGunYu_output_send_offset{cluster="$cluster"})by(input))) > 10485760
```



**QPS**

- QPS环比下跌30%
```
(sum(rate(redisGunYu_output_send_cmd{cluster="$cluster"}[2m]offset 2m)) by(input) - sum(rate(redisGunYu_output_send_cmd{cluster="$cluster"}[2m])) by(input)) / sum(rate(redisGunYu_output_send_cmd{cluster="$cluster"}[2m]offset 2m)) by(input) > 0.3
```

- 成功QPS比例小于90%
```
sum(rate(redisGunYu_output_sender{cluster="$cluster", result="ok"}[2m])) by (input) / sum(rate(redisGunYu_output_sender{cluster="$cluster"}[2m])) by (input)  < 0.9
```





### 日志

日志可以打印到标准输出或者文件，具体参考[配置文件](configuration_zh.md#日志)

打印日志如下
```
2024-04-18T10:47:52.345107184+08:00	info	[RedisOutput(0)] sending
2024-04-18T10:47:52.345107184+08:00	error	[RedisOutput(0)] error : something is wrong
2024-04-18T10:47:52.345107184+08:00	panic	[RedisOutput(0)] xxx
```

