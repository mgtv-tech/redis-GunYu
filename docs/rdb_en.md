# RDB command

- [RDB command](#rdb-command)
  - [Replay RDB file to redis](#replay-rdb-file-to-redis)
  - [Parse RDB file and print key/value](#parse-rdb-file-and-print-keyvalue)


## Replay RDB file to redis 

RDB command can load RDB file to a running redis server or redis cluster, and filter data by policies.

We launch it with a configuration file.   
```
./redisGunYu -cmd=rdb -conf=config/rdb_load.yaml
```
It parse `/tmp/test.rdb` file and load data into the redis cluster(`127.0.0.1:6379,127.0.0.1:6479`), ignore DB 1 and the keys of test_ignore prefix.


It can be launched with command line argument.
```
./redisGunYu -cmd=rdb -rdb.action=load -rdb.rdbPath=/tmp/test.rdb -rdb.load.redis.addresses=127.0.0.1:6379,127.0.0.1:6479 -rdb.load.redis.type=cluster -rdb.load.filter.dbBlacklist=1 -rdb.load.filter.keyFilter.prefixKeyBlacklist=test_ignore
```


**Configuration**

Configuration sections
- action : sub command
- rdbPath : RDB file path
- load : 
  - redis : refer to [redis configuration](sync_configuration_en.md#redis-configuration)
  - replay : refer to [replay configuration](sync_configuration_en.md#replay-configuration)
  - filter : refer to [filter configuration](sync_configuration_en.md#filter-configuration)


A demo configuration
```
action: load
rdbPath: /tmp/test.rdb
load:
  redis:
    addresses: [127.0.0.1:6379,127.0.0.1:6479]
    type: cluster
  filter:
    dbBlacklist: 1
    keyFilter:
      prefixKeyBlacklist: test_ignore
```


## Parse RDB file and print key/value

Parse /tmp/dump.rdb, and print to stdout
```
./redisGunYu -cmd=rdb -rdb.action=print -rdb.rdbPath=/tmp/dump.rdb
```

**Configuration**

Configurations:
- action : sub command
- rdbPath : RDB file path
- print : 
  - output : output file, default is stdout
  - noLogKey : don't print key, default value is false
  - noLogValue : don't print value, default value is false


Demo
```
./redisGunYu -cmd=rdb -rdb.action=print -rdb.rdbPath=/tmp/dump.rdb -rdb.print.output=/tmp/rdb.log -rdb.print.noLogKey=true -rdb.print.noLogValue=true
```


