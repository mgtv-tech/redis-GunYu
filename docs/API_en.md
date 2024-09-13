
# API


If command is sync, `redisGunYu` supports below APIs.



- [API](#api)
  - [Process](#process)
    - [Stop Process](#stop-process)
  - [Synchronization](#synchronization)
    - [Restart Sync Progress](#restart-sync-progress)
    - [Pause Sync](#pause-sync)
    - [Resume Sync](#resume-sync)
    - [Sync Status Information](#sync-status-information)
    - [Sync Configuration Information](#sync-configuration-information)
    - [Full Sync](#full-sync)
    - [Hand over leadership](#hand-over-leadership)
  - [Recycle Local Cache](#recycle-local-cache)
  - [Observability](#observability)
    - [Prometheus Metrics API](#prometheus-metrics-api)


HTTP API are supported to perform relevant devops operations, such as metric collection, process stop, full sync, etc.


## Process

### Stop Process

DELETE http://http_server:port/

```
curl -XDELETE http://http_server:port/
```

Or sending a signal
```
Kill $PID
```

By default, the service will stop the process gracefully, so it will wait for all resources to be reclaimed before exiting. You can configure the `server.gracefullStopTimeout` in the configuration file to set the graceful wait timeout (default is 5 seconds).


## Synchronization

### Restart Sync Progress

POST http://http_server:port/syncer/restart
```
curl -XPOST http://http_server:port/syncer/restart
```


### Pause Sync
```
curl -XPOST 'http://server:port/syncer/pause?inputs=inputIP&flushdb=yes'
```
URL, query parameters:
- inputs: The source Redis IPs that need to be fully synchronized. If all source nodes need to be fully synchronized, write "inputs=all". If there are multiple source IPs, separate them with commas.


### Resume Sync
```
curl -XPOST 'http://server:port/syncer/resume?inputs=inputIP&flushdb=yes'
```
URL, query parameters:
- inputs: The source Redis IPs that need to be fully synchronized. If all source nodes need to be fully synchronized, write "inputs=all". If there are multiple source IPs, separate them with commas.




### Sync Status Information

GET http://http_server:port/syncer/status
```
curl http://http_server:port/syncer/status
```
Response
```
[
    {
        "Input": "127.0.0.1:16311",   // Source Redis node
        "Role": "leader",             // Leader or follower, leader is responsible for syncing this Redis node(127.0.0.1:16311)
        "Transaction":true,           // Transaction mode
        "State": "run"                // Running state
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


### Sync Configuration Information

Get the configurations of redis-GunYu

Default is YAML format
```
GET http://http_server:port/syncer/config
```
Or JSON format
```
GET http://http_server:port/syncer/config?format=json
```




### Full Sync
```
curl -XPOST 'http://http_server:port/syncer/fullsync?inputs=inputs&flushdb=yes' 
```
URL, query parameters:
- inputs: The source Redis IP and port that need to be fully synchronized. If all source nodes need to be fully synchronized, write "inputs=all". If there are multiple source IP+port, separate them with commas. The specific IP+port can be obtained through the `http://http_server:port/syncer/status` API.
- flushdb: Whether to execute flushdb before the full sync.

When flushdb=yes, if only some of the source nodes are being fully synchronized, you need to ensure that the slots of the source and target Redis can correspond one-to-one, otherwise please fully synchronize all the source nodes (inputs=all).


For example
```
# Force full synchronization of the two source Redis nodes 127.0.0.1:16302 and 127.0.0.1:16310; and clear the data in the corresponding target Redis node (execute flushdb)
curl -XPOST 'http://http_server:port/syncer/fullsync?inputs=127.0.0.1:16302,127.0.0.1:16310&flushdb=yes'
```



### Hand over leadership 

You can use this API to transfer synchronization responsibility(leadership) from one `redisGunYu` node to another when `redisGunYu` is deployed in cluster mode.

```
curl -XPOST 'http://http_server:port/syncer/handover?inputs=inputs' 
```
URL, query parameters:
- inputs: The source Redis IP and port that need to be fully synchronized. If all source nodes need to be fully synchronized, write "inputs=all". If there are multiple source IP+port, separate them with commas. The specific IP+port can be obtained through the `http://http_server:port/syncer/status` API.




## Recycle Local Cache

GET http://http_server:port/storage/gc
```
curl http://http_server:port/storage/gc
```


## Observability
### Prometheus Metrics API

GET http://http_server:port/prometheus