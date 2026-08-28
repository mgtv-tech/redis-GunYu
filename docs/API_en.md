
# API


If command is sync, `redisGunYu` supports below APIs.



- [API](#api)
  - [Process](#process)
    - [Stop Process](#stop-process)
  - [Synchronization](#synchronization)
    - [Paused Startup and Runtime Control](#paused-startup-and-runtime-control)
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

### Paused Startup and Runtime Control

Use `server.initialPaused: true` when a GunYu process must join topology
discovery and HA election before it begins to read from the source Redis or
write to the target Redis. This is useful when an operator needs to inspect
the resolved inputs, confirm the elected roles, or coordinate a controlled
cutover before replication starts.

```yaml
server:
  listen: 127.0.0.1:18001
  initialPaused: true
```

Start GunYu normally, then wait for every expected input to appear in
`/syncer/status` with `State: "pause"`:

```bash
curl http://127.0.0.1:18001/syncer/status
```

At this point, Redis replication input and output pipelines have not started.
Topology discovery and, in HA mode, leader election continue to run. To start
all pipelines managed by this GunYu process, resume every input:

```bash
curl -XPOST 'http://127.0.0.1:18001/syncer/resume?inputs=all'
```

For a selected input, obtain its exact `Input` value from `/syncer/status` and
pass it to `inputs` instead:

```bash
curl -XPOST 'http://127.0.0.1:18001/syncer/resume?inputs=127.0.0.1:16379'
```

Verify that the affected pipelines report `State: "run"` before treating the
cutover as active. `resume` is idempotent, so it is safe for automation to
retry it.

To temporarily stop selected pipelines after they have started, use `pause`:

```bash
curl -XPOST 'http://127.0.0.1:18001/syncer/pause?inputs=all'
```

The selected runtime state is retained when a syncer is recreated by
`/syncer/restart`: a paused input remains paused and a running input resumes
running. In HA mode, resume the inputs on each GunYu process that should be
allowed to replicate; a follower can be paused or running while it waits for
leadership. A later leader handover preserves that process-local state.

`initialPaused` affects only newly initialized inputs. If it is omitted or
`false`, GunYu retains the default behavior and starts synchronization
automatically. Stopping the process uses the normal `DELETE /` API and does
not imply a later resume; start a new process and use `/syncer/status` to
determine its current state.

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

When `server.initialPaused` is enabled, wait until the expected syncers are visible with state `pause`, then use `inputs=all` to start all local pipelines. Resume is idempotent.




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
