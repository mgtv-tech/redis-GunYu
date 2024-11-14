# Configuration of sync command

- [Configuration of sync command](#configuration-of-sync-command)
  - [Configuration File](#configuration-file)
    - [Redis Configuration](#redis-configuration)
    - [Input Redis(Source Redis)](#input-redissource-redis)
    - [Output redis(Target Redis)](#output-redistarget-redis)
      - [Replay configuration](#replay-configuration)
      - [Filter configuration](#filter-configuration)
    - [Cache](#cache)
    - [Cluster](#cluster)
    - [Logging](#logging)
    - [Server](#server)
  - [Configuration File Examples](#configuration-file-examples)
    - [Minimum Configuration File](#minimum-configuration-file)
    - [Detailed Configuration](#detailed-configuration)
  - [Command Line Arguments](#command-line-arguments)



`redisGunYu` can be launched with either a configuration file or command line arguments.


## Configuration File

The configuration file consists of several sections:
- input: Configuration for the input Redis (source) endpoint.
- output: Configuration for the output Redis (target) endpoint.
- channel: Local cache configuration.
- cluster: Cluster mode configuration.
- log: Logging configuration.
- server: Server-related configuration.
- filter: Filter strategy configuration.


### Redis Configuration

Redis configuration:
- addresses: Redis addresses, an array. If Redis is deployed as a cluster, it is recommended to configure more than one IP address in `addresses` to avoid the case that `redis-GunYu` can't connect to the Redis cluster in case of a node failure.
- userName: Redis username.
- password: Redis password.
- type: Redis type.
  - standalone: Synchronize based on the addresses in the `addresses` field.
  - cluster: Redis cluster
- clusterOptions:
  - replayTransaction: Whether to attempt using transactions (pseudo-transactions, not based on multi/exec, but sending Redis commands as a package) for synchronization. Enabled by default.
- keepAlive: Maximum number of connections per Redis node.
- aliveTime: Connection keep-alive timeout.


### Input Redis(Source Redis)


The input configuration is as follows:
- redis: Redis configuration.
- rdbParallel: Limit on the number of RDB operations at the same time. No limit by default.
- mode:
  - static: Synchronize only the nodes configured in the Redis configuration.
  - dynamic: If Redis is a cluster, synchronize all nodes in the Redis cluster.
- syncFrom:
  - prefer_slave: Prefer synchronizing from the slave. If the slave is not available, synchronize from the master.
  - master: Synchronize from the master.
  - slave: Synchronize from the slave.


### Output redis(Target Redis)

The output configuration is as follows:
- redis: Redis configuration.
- replay: refere to [replay](#replay-configurations)
- filter: refer to [filter](#filter-configurations)


> The synchronization delay depends on `batchCmdCount` and `batchTicker`. redis-GunYu packages commands, and then sends them to the target endpoint as long as one of the two configurations is satisfied.



#### Replay configuration
- replay: 
  - resumeFromBreakPoint: Enable or disable breakpoint resumption. Enabled by default.
  - keyExists: Behavior when the key already exists in the output.
    - replace: Replace the key (default).
    - ignore: Ignore the key.
    - error: Throw an error and stop synchronization.
  - keyExistsLog: Logging behavior(disabled by default).
    - true: If `keyExists` is "replace," log an info message when replacing the key; if `keyExists` is "ignore," log a warning message when replacing the key.
    - false: Disable `keyExists` logging.
  - functionExists: Behavior for replaying function fields, similar to the `FUNCTION RESTORE` command parameters.
    - flush:
    - replace:
  - maxProtoBulkLen: Maximum size of the protocol's buffer, referring to the Redis configuration `proto-max-bulk-len`. The default is 512 MiB.
  - targetDbMap : DB mapping, map structure, e.g., syncing DB0 to DB1, DB2 to DB3， `{"targetDbMap":{"0":1,"2":3}}`
  - targetDb: Which database will be synced. The default value is -1, it is all database of input redis.
  - batchCmdCount: Number of commands for batching(default: 100).
  - batchTicker: Waiting time for batching(default: 10ms).
  - batchBufferSize: Buffer size for batching. `redis-GunYu` will flush the batch buffer if one of them is satisfied.
  - replayRdbParallel: Number of threads used for replaying RDB. The default is the CPU count multiplied by 4.
  - updateCheckpointTicker: Default: 1 second.
  - keepaliveTicker: Default: 3 seconds. Interval for keeping the heartbeat.
  - enableAofPipeline : Replay commands in a pipeline. Send command and receive reply in different threads, while it can speed up data synchronization, may lead to data inconsistency. Enable this feature with caution.


#### Filter configuration

- filter:
  - commandBlacklist: Command blacklist
  - keyFilter: Filtering keys
    - prefixKeyBlacklist: Prefix key blacklist
    - prefixKeyWhitelist: Prefix key whitelist
  - slotFilter: Filtering slots keys
    - keySlotBlacklist : slots blacklist
    - keySlotWhitelist : slots whitelist



**example of filter configuration**

not synchronizing `del` commands and keys starting with `redisGunYu`:
```
output:
  filter:
    commandBlacklist:
      - del
    keyFilter:
      prefixKeyBlacklist: 
        - redisGunYu
    slotFilter:
      keySlotWhitelist: 
        - [0,1000]
        - [1002] 
```


### Cache

Configuration:
- storer: Storage for RDB and AOF
  - dirPath: Storage directory, default is `/tmp/redis-gunyu`
  - maxSize: Maximum storage size, in bytes, default is 50GiB
  - logSize: Size of each AOF file, default is 100MiB
  - flush: Strategy for flushing AOF files to disk, default is auto
    - duration: Interval for flushing AOF files
    - everyWrite: Synchronize after each command write to AOF
    - dirtySize: Synchronize when AOF file data exceeds dirtySize
    - auto: Depends on operating system
- verifyCrc: Default is false
- staleCheckpointDuration: The checkpoints which are older than staleCheckpointDuration are expired, default is 12 hours




### Cluster

`redisGunYu` supports cluster mode, ensuring the high availability of `redisGunYu`. In case of a leader role failure, the follower role takes over its tasks seamlessly.


Cluster mode configuration:
- groupName: Cluster name, the name must be unique.
- metaEtcd: etcd configuration [optional]. If `metaEtcd` is empty, source redis will be used to implement locking and registry.
  - endpoints: etcd node addresses
  - username: username
  - password: password
- leaseTimeout: Leader lease, if the leader does not renew within leaseTimeout, it means the leader has expired and a new election will be started; default is 10 seconds, value range is [3s, 600s]
- leaseRenewInterval: Leader lease renewal interval, default is 3.33 seconds, generally chosen as 1/3 of leaseTimeout, value range is [1s, 200s]


Configuration example:
```
cluster:
  groupName: redisA
  leaseTimeout: 9s
  metaEtcd: 
    endpoints:
      - 127.0.0.1:2379
```


### Logging

Logging configuration:
- level: Log level, default is info; levels include debug, info, warn, error, panic, fatal
- handler:
  - file: Log output to file
    - fileName: File name
    - maxSize: File size in MiB
    - maxBackups: Maximum number of log files
    - maxAge: Number of days to retain log files
  - stdout: Log output to standard output by default
- withCaller: Whether logs include the source code file name, default is false
- withFunc: Whether logs include the caller function, default is false
- withModuleName: Whether logs include the module name, default is true


### Server

Server configuration:
- listen: Listening address, default is "127.0.0.1:18001"
- listenPeer: Used for communication with other `redis-GunYu` processes, IP:Port, default is the same as listen. Note: Do not use 127.0.0.1.
- metricRoutePath: Prometheus HTTP path, default is "/prometheus"
- checkRedisTypologyTicker: Time interval for checking Redis cluster topology, default is 30 seconds, can be specified as 1s, 1h, 1ms, etc.
- gracefullStopTimeout: Graceful shutdown timeout, default is 5 seconds


## Configuration File Examples


### Minimum Configuration File

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



### Detailed Configuration
```
server:
  listen: 0.0.0.0:18001 
  listenPeer: 10.220.14.15:18001  # LAN address
input:
  redis:
    addresses: [127.0.0.1:6300]
    type: cluster
  mode: dynamic
  syncFrom: prefer_slave
channel:
  storer:
    dirPath: /tmp/redisgunyu-cluster
    maxSize: 209715200
    logSize: 20971520
  staleCheckpointDuration: 30m
output:
  redis:
    addresses: [127.0.0.1:6310]
    type: cluster
  replay:
    resumeFromBreakPoint: true
log:
  level: info
  handler:
    stdout: true
  withModuleName: false

# Cluster mode requires the following cluster configuration
cluster:
  groupName: redis1
  leaseTimeout: 3s
```



## Command Line Arguments

We can start redisGunYu with command line arguments as follows:
```
redisGunYu --sync.input.redis.addresses=127.0.0.1:6379 --sync.output.redis.addresses=127.0.0.1:16379
```

The argument names are prefixed with `--sync.` and followed by the configured field names, connected with a dot (`.`). Array values are separated by commas (`,`).

For example, if the source Redis addresses are configured as follows in the configuration file:
```
input:
  redis:
    addresses: [127.0.0.1:6379, 127.0.0.2:6379]
```

The corresponding command line argument would be `--sync.input.redis.addresses=127.0.0.1:6379,127.0.0.2:6379`.

For example, if the slots white list are configured as follows in the configuration file:
```
output:
  filter:
    slotFilter:
      keySlotWhitelist: 
        - [0,1000]
        - [1002] 
```
The corresponding command line argument would be`--sync.output.filter.slotFilter.keySlotWhitelist=[0,1000],[1002]`

You can use `redisGunYu -h` to view all available arguments.


