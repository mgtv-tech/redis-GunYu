# 部署


- [部署](#部署)
  - [部署redis-GunYu](#部署redis-gunyu)
    - [单节点部署](#单节点部署)
    - [集群部署](#集群部署)
  - [可观测性](#可观测性)
    - [监控](#监控)



## 部署redis-GunYu

### 单节点部署

单节点部署，配置文件不用配置`cluster`相关的配置。直接启动即可
```
redisGunYu -conf config.yaml
```


### 集群部署


由于现阶段的集群部署依赖etcd，所以集群部署需要先部署etcd集群。

再修改配置文件，增加`cluster`相关配置，参考[集群配置](configuration_zh.md#集群)，或其[demo配置](configuration_zh.md#较完善配置)


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
