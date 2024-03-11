# redis GunYu

## 简介

`redis-GunYu`是一款redis数据治理工具，可以进行数据实时同步，数据迁移，备份、校验恢复，数据分析等等。


## 特性

### 数据实时同步

`Redis-GunYu`支持对redis进行实时同步，其同步功能有以下特性：


**数据同步**

- 全量同步+增量同步 ： 根据偏移量以及复制成本进行判断，是全量同步还是增量同步
- 断点续传：由于网络抖动、redis节点重启、redis机器重启等，造成的连接中断，恢复后支持从本地缓存进行同步，避免频繁对源端进行RDB操作
- 源端和目标端redis拓扑 ： 支持单机模式和cluster模式，以及混合模式
- 拓扑变化 ： 实时监听源和目标端redis拓扑变更（如加减节点，主从切换等等），以更改一致性策略和调整其他功能策略
- 同步粒度：同步整个redis集群或指定某些节点
- 复制优先级：可用指定优先从从库进行复制或主库复制
- 并发 ： 在保证一致性的前提下，并发地进行数据回放
- 数据过滤：可以对某些key、db、命令等进行过滤
- key回放策略 ： 
  - 全量同步、增量同步时，已存在key根据策略进行回放
  - 支持大key：拆分进行回放


**可用性**

- 工具高可用 ： 支持主从模式，以最新记录进行自主选举，自动和手动failover
- 本地缓存 ： 对存储在本地的数据文件，支持基本的数据CRC校验


**一致性**

- 当源端和目标端分片信息一致时，采用伪事务方式批量写入，实时更新偏移，最大可能保证一致性
- 当源端和目标端分片不一致时，采用非事务批量写入，定期更新偏移




### 其他

其他功能，仍在开发中，敬请期待。


## 快速开始

### 安装

**下载二进制**



**编译源码**

先确保已经安装Go语言，配置好环境变量

```
git clone https://github.com/mgtv-tech/redis-gunyu.git
cd redis-GunYu
make
```
在本地生成`redisGunYu`二进制文件。


### 使用

```
./redisGunYu -conf ./config.yaml
```
对于配置文件，请见[配置](docs/configuration_zh.md)


## 文档

- [配置](docs/configuration_zh.md)
- [部署](docs/deployment_zh.md)
- [API](docs/API_zh.md)
- [注意事项](docs/attentions_zh.md)



## 贡献

欢迎大家一起来完善redis-GunYu。如果您有任何疑问、建议或者想添加其他功能，请直接提交issue或者PR。

请按照以下步骤来提交PR：
- 克隆仓库
- 创建一个新分支：如果是新功能分支，则命名为feature-xxx；如果是修复bug，则命名为bugfix-xxx
- 在PR中详细描述更改的内容


## 许可

`redis-GunYu`是基于Apache2.0许可的，请见[LICENSE](LICENSE)。


## 联系

如果您有任何问题，请联系`ikenchina@gmail.com`。
