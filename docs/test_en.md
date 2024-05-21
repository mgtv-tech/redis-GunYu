# Testing

- [Testing](#testing)
  - [Tests](#tests)
    - [Redis-related Tests](#redis-related-tests)
      - [RDB Test](#rdb-test)
      - [AOF Test](#aof-test)
      - [Command Test](#command-test)
      - [Version Compatibility Test](#version-compatibility-test)
  - [API Testing](#api-testing)
  - [Service Testing](#service-testing)
  - [Consistency Testing](#consistency-testing)
    - [Offline Testing](#offline-testing)

## Tests

### Redis-related Tests

#### RDB Test

| Data Structure | RDB Type | Test Result |
| :- | :- | :- |
| string | string | Passed |
| list | RDBTypeListZiplist | Passed |
| list | RDBTypeList | Passed |
| list | RDBTypeQuicklist | Passed |
| list | RDBTypeQuicklist2 | Passed |
| set | RDBTypeSet | Passed |
| set | RDBTypeSetIntset | Passed |
| set | RDBTypeSetListpack | Passed |
| zset | RDBTypeZSet | Passed, Redis 2.0 data structure, no need for testing |
| zset | RDBTypeZSet2 | Passed |
| zset | RDBTypeZSetZiplist | Passed |
| zset | RDBTypeZSetListpack | Passed |
| hash | RDBTypeHashZiplist | Passed |
| hash | RDBTypeHashZipmap | Passed |
| hash | RDBTypeHash | Passed |
| hash | RDBTypeHashListpack | Passed |
| stream | RDBTypeStreamListPacks | Passed |
| stream | RDBTypeStreamListPacks2 | Passed |
| stream | RDBTypeStreamListPacks3 | Passed |
| function | RDBFunction2 | Passed |
| aux | expire | Passed |
| aux | select | Passed |
| Non-restore mode | Splitting RDB data structure and replaying via redis commands | Passed |
| File corruption | File corruption detection | Passed |
| Multiple DBs | Multiple Redis DBs | Passed |

#### AOF Test

| Category | Test Case | Test Result |
| :- | :- | :- |
| File corruption | Detecting corrupted AOF file | Passed |
| Data structure test | string | Passed |
| Data structure test | list | Passed |
| Data structure test | hash | Passed |
| Data structure test | set | Passed |
| Data structure test | zset | Passed |
| Data structure test | stream | Passed |

#### Command Test

| Command Category | Test Command | Test Result |
| :- | :- | :- |
| General commands | COPY, MOVE, RENAME*, RESTORE | Passed |
| hash | HSET, HDEL, HINCRBY*, HSETNX | Passed |
| list | BLMOVE, BLMPOP, BL*, BR*, LINSERT, LMOVE, LMPOP, LPOP*, LPUSH*, LREM, LSET, LTRIM, RPOP*, RPUSH* | Passed |
| pub/sub | PUBLISH, PSUBSCRIBE, SPUBLISH, SSUBSCRIBE | Passed |
| script | FUNCTION LOAD, FUNCTION DELETE, FUNCTION FLUSH, SCRIPT LOAD, SCRIPT FLUSH | Passed |
| set | SADD, SMOVE, SPOP, SREM | Passed |
| zset | ZADD, ZINCRBY, ZMPOP, ZPOP* | Passed |
| stream | XADD, XACK, XCLAIM, XDEL, XGROUP*, XTRIM | Passed |
| string | SET*, DEL, EXPIRE*, APPEND, DECR, INCR*, MSET*, PSETEX | Passed |
| transaction | MULTI, EXEC | Passed |

#### Version Compatibility Test

Testing the compatibility between different versions of RDB and AOF; RDB testing includes both RESTORE and non-RESTORE command.

| Source Version | Target Version | Test Result | Remarks |
| :- | :- | :- | :- |
| 4.0 | 5.0 | Passed | |
| 4.0 | 6.0 | Passed | |
| 4.0 | 7.0 | Passed | |
| 5.0 | 6.0 | Passed | |
| 5.0 | 7.0 | Passed | |
| 6.0 | 7.0 | Passed | |
| 7.0 | 6.0 | Passed | Redis versions below 7.0 do not support FUNCTION command, `redis-GunYu` ignores synchronizing this data structure |
| 7.0 | 5.0 | Passed | Redis versions below 7.0 do not support FUNCTION command, `redis-GunYu` ignores synchronizing this data structure |
| 7.0 | 4.0 | Passed | Redis versions below 7.0 do not support FUNCTION command, `redis-GunYu` ignores synchronizing this data structure |
| 6.0  | 5.0  | Passed  |   |
| 6.0  | 4.0  | Passed  |   |
| 5.0  | 4.0  | Passed  |   |



## API Testing
| Category | Test Case | API | Test Result |
| :- | :- | :- | :- |
| Process | Process Exit | curl -XDELETE http://server:port/ | Passed |
| Storage | Trigger Garbage Collection | curl http://server:port/storage/gc | Passed |
| Synchronization | Restart Sync Progress | curl -XPOST http://server:port/syncer/restart | Passed |
| Synchronization | Revoke Leadership | curl -XPOST http://server:port/syncer/handover | Passed |
| Synchronization | Force Full Sync | curl -XPOST 'http://server:port/syncer/fullsync?inputs=inputIP&flushdb=yes' -v | Passed |
| Synchronization | Sync Status | curl http://server:port/syncer/status | Passed |
| Synchronization | Pause Sync | curl -XPOST http://server:port/syncer/pause | Passed |
| Synchronization | Resume Sync | curl -XPOST http://server:port/syncer/resume | Passed |



## Service Testing

| Category | Test Item | Test Case | Test Result | Remarks |
| :- | :- | :- | :- | :- |
| High Availability | Master-Slave Auto Switching | | Passed | |
| High Availability | Leadership Migration | | Passed | |
| High Availability | Leader Lease Renewal Failure | | Passed | |
| High Availability | Campaign Failure | | Passed | |
| Process Exit | Graceful Exit | | Passed | |
| Process Exit | Signal Exit | | Passed | |
| Process Exit | Interface Exit | | Passed | |
| Cluster | Both Sides are Cluster Clusters | SyncFrom configuration: master, slave, prefer_Slave.  | Passed | |
| Cluster | Syncing from Standalone redis to Cluster | | Passed | |
| Cluster | Syncing from Cluster to Standalone redis | | Passed | |
| Cluster | Topology Periodic Check | Detect changes and modify sync strategy | Passed | |
| Cache | Garbage Collection | Recycle the oldest logs when storage exceeds limitation | Passed | |
| Cache | Consistency Check | Check for corrupted files | Passed | |
| Cache | Flush Policy | | Passed | |
| Target Redis | Topology Change | Transaction mode: Switch to non-transaction mode upon receiving MOVE and ASK errors | Passed | |
| Target Redis | Topology Change | Transaction mode: Periodically check for topology changes and attempt to switch to non-transaction mode | Passed | |
| Target Redis | Topology Change | Non-transaction mode: Periodically check for transaction changes and attempt to switch to transaction mode | Passed | |
| Target Redis | Failure | Master Failure | Passed | |
| Target Redis | Manual Failover | | Passed | |
| Target Redis | Slot Migration | | Passed | |
| Source Redis | Node Selection | Sync from nodes with roles such as master, slave, prefer_slave based on configuration | Passed | |
| Source Redis | Redis Master-Slave Switching | | Passed | |
| Source Redis | Add Node to Cluster | | Passed | |
| Source Redis | SLOT Change, Transaction Switch | | Passed | |
| Source Redis | SLOT in Migration State | | Passed | |
| Source Redis | Failure | Master Failure | Passed | Continue sync after cluster elects new master |
| Source Redis | Failure | Slave Failure | Passed | Wait for cluster to mark node as fail |
| Resource | CPU | CPU usage | Passed | |
| Resource | Memory | No memory leaks | Passed | |
| Resource | Concurrency Safety | Check data concurrency safety | Passed | |
| Observability | Prometheus Metrics |  | Passed | |
| Observability | Logs | Validity of log configurations | Passed | |
| Observability | Logs | No missing logs | Passed | |



## Consistency Testing

### Offline Testing


**Test Purpose**

Test data consistency between the source and target redis.

**Test Environment**
- Machine: 16 cores, 32GB memory
- Data Structures: key(set/del), set(sadd, spop), list(lpush, lpop), incr, hash(hset, hdel)
- QPS: 4500
- Redis: Source (3 masters, 3 slaves), Target (3 masters, 1 slave)
- Data Volume: 260,000 keys

| Sync Type | Case | Test Method | Test Result | Description |
| :- | :- | :- | :- | :- |
| Transaction | Source Master-Slave Switching | Execute `cluster failover force` in redis, compare source and target redis via `redis-full-check` tool | Passed | Consistent data after 10+ switches |
| Transaction | Target Master-Slave Switching | Execute `cluster failover force` in redis, compare source and target redis via `redis-full-check` tool | Passed | Consistent data after 10+ switches |
| Non-Transaction | Source Master-Slave Switching | Execute `cluster failover force` in redis, compare source and target redis via `redis-full-check` tool | Passed | Consistent data after 10+ switches |
| Non-Transaction | Target Master-Slave Switching | Execute `cluster failover force` in redis, compare source and target redis via `redis-full-check` tool | Passed | Consistent data after 10+ switches |
| Slot migration | Migrating source redis slots | Scale up/down source redis, or migrate slots |   **Failed**  | A little bit of data is consistent |
| Slot migration | Migrating target redis slots | Scale up/down source redis, or migrate slots |   Passed  |   |


> For the case that data is inconsistent after migrated source redis slots, please refers to the solution [inconsistency solution](attentions_en.md#data-is-inconsistent-after-migrated-slots)

