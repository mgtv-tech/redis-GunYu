
# Attentions

- [Attentions](#attentions)
  - [Redis Version](#redis-version)
    - [Source and Target](#source-and-target)
  - [Command Compatibility](#command-compatibility)
  - [Full Synchronization](#full-synchronization)
  - [Data is inconsistent after migrated slots](#data-is-inconsistent-after-migrated-slots)

## Redis Version

redis-GunYu supports Redis versions from 4.0 to 7.2.

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


## Full Synchronization

Before `redisGunYu` fully synchronizes the RDB to the target node, it will not clean up the data on the target node, but will directly replay the RDB data. This means that the data on the target node may be more than the source node.

So, if you want to make data consistent, please manually send flushdb and full synchronization. Please refer to the [Forced Full Synchronization API](API_en.md#full-sync).

Reason:
- If the slots of the source and target Redis do not correspond (cross slots), you only perform a full synchronization on one of the nodes, then `redis-GunYu` cannot execute the flushdb command on the target Redis.
- If you execute flushdb, during the process of synchronizing the RDB data to the target Redis, some keys in the target Redis data may not exist, causing inconsistency.
- RDB replay may fail, causing the target node to lack data.


## Data is inconsistent after migrated slots

During the migration of SLOT of source redis cluster, Redis will send the restore command to the destination node, and then execute the delete command on the original node. During synchronization, redis-GunYu maintains two pipelines, so there may be an asynchronous execution problem. When synchronized to the destination node, the restore may be executed first, and then the delete operation, so the key may not exist on the destination node.


**Solution**

1. Perform a full synchronization, for example, migrated slot 10 from node A to node B, then perform a full synchronization on node B, refer to the [Forced Full Synchronization API](API_en.md#forced-full-synchronization).
2. Modify the Redis scaling up, scaling down, and slot migration scripts.

