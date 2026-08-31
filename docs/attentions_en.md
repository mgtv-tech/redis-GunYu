
# Attentions

- [Attentions](#attentions)
  - [Redis Version](#redis-version)
    - [Source and Target](#source-and-target)
  - [Command Compatibility](#command-compatibility)
  - [Full Synchronization](#full-synchronization)
  - [Sentinel Failover Consistency](#sentinel-failover-consistency)
  - [Data is inconsistent after migrated slots](#data-is-inconsistent-after-migrated-slots)

## Redis Version

redis-GunYu supports Redis versions from 4.0 to 8.x.

Redis 8 compatibility now includes RDB version 13 and the Redis 8 cluster `SLOT_INFO` metadata opcode.

### Source and Target 

It is best if the source and target Redis have the same version, because:
1. RDB Replay: The restore command across different versions may be incompatible. If the restore fails, `redis-GunYu` will try to replay the RDB data using the Redis commands. That is pretty slow.
2. Scaling and Slot Migration: During data migration, the restore-asking command will be sent, which may also be incompatible.


For Redis compatibility, please see the [Test Document](test_en.md#version-compatibility-test).


## Command Compatibility

The following commands are not supported:
- flush*
- bgsave, save
- cluster


**EVALSHA: In Redis 4, if AOF is not enabled on the source Redis, the EVALSHA command may fail**

If AOF is disabled on the source Redis and the target Redis does not have the Lua script cached, then an EVALSHA command executed on the source Redis cannot be executed on the target Redis.
To avoid this issue, please enable AOF on the source Redis or upgrade Redis.



## Full Synchronization

Before `redisGunYu` fully synchronizes the RDB to the target node, it will not clean up the data on the target node, but will directly replay the RDB data. This means that the data on the target node may be more than the source node.

So, if you want to make data consistent, please manually send flushdb and full synchronization. Please refer to the [Forced Full Synchronization API](API_en.md#full-sync).

Reason:
- If the slots of the source and target Redis do not correspond (cross slots), you only perform a full synchronization on one of the nodes, then `redis-GunYu` cannot execute the flushdb command on the target Redis.
- If you execute flushdb, during the process of synchronizing the RDB data to the target Redis, some keys in the target Redis data may not exist, causing inconsistency.
- RDB replay may fail, causing the target node to lack data.

## Sentinel Failover Consistency

Redis GunYu periodically resolves Sentinel topology and rebuilds the affected
syncer when the selected source node or target master changes. It reuses the
existing replication ID, local channel, and checkpoint recovery flow and falls
back to a full sync when partial resynchronization is not possible.

Sentinel failover is based on asynchronous Redis replication. A write accepted
by the old master but not replicated to the promoted replica may be lost. The
system remains eventually/weakly consistent; Sentinel support does not provide
zero RPO, exactly-once replay, or strong consistency. Validate non-idempotent
workloads using business values and checkpoint state after failover.


## Data is inconsistent after migrated slots

During the migration of SLOT of source redis cluster, Redis will send the restore command to the destination node, and then execute the delete command on the original node. During synchronization, redis-GunYu maintains two pipelines, so there may be an asynchronous execution problem. When synchronized to the destination node, the restore may be executed first, and then the delete operation, so the key may not exist on the destination node.


**Solution**

1. Perform a full synchronization, for example, migrated slot 10 from node A to node B, then perform a full synchronization on node B, refer to the [Forced Full Synchronization API](API_en.md#forced-full-synchronization).
2. Modify the Redis scaling up, scaling down, and slot migration scripts.
