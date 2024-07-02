# Deployment


- [Deployment](#deployment)
  - [Deployment of redis-GunYu](#deployment-of-redis-gunyu)
    - [Standalone Deployment](#standalone-deployment)
    - [Cluster Deployment](#cluster-deployment)
  - [Observability](#observability)
    - [Monitoring](#monitoring)
      - [Monitoring Metrics and Alerts](#monitoring-metrics-and-alerts)
    - [Logging](#logging)



## Deployment of redis-GunYu

### Standalone Deployment

For standalone deployment, `cluster` section of configuration file must be empty. 
You can run below command directly.
```
./redisGunYu -conf ./config.yaml
```


### Cluster Deployment

Adding `cluster` section to configuration file, refer to [Cluster Configuration](configuration_en.md#cluster), or the [demo configuration](configuration_en.md#detailed-configuration).


> Cluster deployment is recommending for production deployment.




## Observability

### Monitoring

`redis-GunYu` supports Prometheus metric, you can get the metrics via `GET http://http_server:port/prometheus`.  



**Deployment Steps**

- Edit the Prometheus configuration file, such as prometheus.yaml, and add the `redis-GunYu` job with the additional cluster label.
```
scrape_configs:
  - job_name: "redisGunYu"
    metrics_path: /prometheus
    static_configs:
      # For clusterA cluster of redis-GunYu, differentiated by the cluster label
      - targets: ["localhost:19000","localhost:18000"]
        labels:
          cluster: 'clusterA'
      # For clusterB cluster of redis-GunYu
      - targets: ["localhost:28000"]
        labels:
          cluster: 'clusterB'
```
- Import the [`grafana_en.json`](../deploy/grafana_en.json) from the deploy directory into Grafana (click the plus icon in the Grafana page, select `import dashboard`).


**Note**

To view the synchronization delay over time, you need to specify the key for synchronization delay testing `syncDelayTestKey` in the configured `input`, to avoid conflicts with the keys in Redis data.
```
input:
  syncDelayTestKey: redis-GunYu-syncDelay-testKey
```
> `redis-GunYu` will periodically write data to this key in the source Redis, and then calculate the time interval when it is synchronized to the target Redis.


Please refer to [Synchronization Delay Configuration](configuration_en.md#output).



#### Monitoring Metrics and Alerts

Below are some example alerts. Adjust the thresholds according to your requirements.

**Process**

- Process failure
```
up{cluster="$cluster"} == 0
```

- Process memory usage exceeds 1GB
```
process_resident_memory_bytes{cluster="$cluster"} > 1073741824
```

- Concurrency exceeds 2000
```
go_goroutines{cluster="$cluster"} > 2000
```


**Synchronization Counts Alerts**

- More than 1 full sync within 10 minutes
```
(redisGunYu_input_sync_type{cluster="$cluster", sync_type="full"} OR on() vector(0)) - (redisGunYu_input_sync_type{cluster="$cluster", sync_type="full"} offset 10m OR on() vector(0)) > 1
```

- More than 2 sync process starts within 10 minutes
```
(redisGunYu_input_sync_type{cluster="$cluster"} OR on() vector(0)) - (redisGunYu_input_sync_type{cluster="$cluster"} offset 10m OR on() vector(0)) > 2
```


**Synchronization Delay Alerts**

- Delay exceeds 1 second
```
# redisGunYu_output_sync_delay is in nanoseconds
max(redisGunYu_output_sync_delay{cluster="$cluster"}) > 1000000000
```

- Delay exceeds 10MB
```
max(abs(sum(redisGunYu_input_offset{cluster="$cluster"}) by(input) - sum(redisGunYu_output_send_offset{cluster="$cluster"})by(input))) > 10485760
```



**QPS**

- QPS dropped by 30% compared to the previous period
```
(sum(rate(redisGunYu_output_send_cmd{cluster="$cluster"}[2m]offset 2m)) by(input) - sum(rate(redisGunYu_output_send_cmd{cluster="$cluster"}[2m])) by(input)) / sum(rate(redisGunYu_output_send_cmd{cluster="$cluster"}[2m]offset 2m)) by(input) > 0.3
```

- Success rate of QPS is less than 90%
```
sum(rate(redisGunYu_output_sender{cluster="$cluster",result="ok"}[2m])) by (input) / sum(rate(redisGunYu_output_sender{cluster="$cluster"}[2m])) by (input)  < 0.9
```



### Logging

Logs can be redirected to standard output or to a file. Please refer to the [Configuration File](configuration_en.md#logging) for details.

Sample log output:
```
2024-04-18T10:47:52.345107184+08:00	info	[RedisOutput(0)] sending
2024-04-18T10:47:52.345107184+08:00	error	[RedisOutput(0)] error : something is wrong
2024-04-18T10:47:52.345107184+08:00	panic	[RedisOutput(0)] xxx
```