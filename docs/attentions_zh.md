# 注意事项

## redis版本

redis-GunYu支持从4.0到7.2的redis版本。

### 源端和目的端

源和目标redis集群的版本要求一致，主要由于：
1. RDB回放：restore命令有版本要求；当然，如果restore失败，`redis-GunYu`会尝试通过redis命令的方式对RDB数据进行回放。
2. 扩容和slot迁移：数据迁移时，会发送restore-asking命令，也有版本要求


## 命令兼容性

不支持的命令如下
- flush*
- bgsave， save
- cluster



