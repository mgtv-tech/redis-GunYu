# 部署


## 可观测性

### 指标

`redis-GunYu`支持prometheus指标收集接口，`GET http://http_server:port/prometheus`。  



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
- 将deploy目录下的`grafana-10.2.3.json`导入到grafana。(grafana页面右上角加号，选择`import dashboard`)



