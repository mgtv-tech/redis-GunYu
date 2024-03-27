# 

[![CI](https://github.com/mgtv-tech/redis-GunYu/workflows/goci/badge.svg)](https://github.com/mgtv-tech/redis-GunYu/actions/workflows/goci.yml)
[![LICENSE](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/mgtv-tech/redis-GunYu/blob/master/LICENSE)
[![release](https://img.shields.io/github/release/mgtv-tech/redis-GunYu)](https://github.com/mgtv-tech/redis-GunYu/releases)


- [](#)
  - [简介](#简介)
  - [特性](#特性)
    - [数据实时同步](#数据实时同步)
    - [其他](#其他)
  - [产品比较](#产品比较)
  - [实现](#实现)
  - [快速开始](#快速开始)
    - [安装](#安装)
    - [使用](#使用)
  - [文档](#文档)
  - [贡献](#贡献)
  - [许可](#许可)
  - [联系](#联系)



## 简介

`redis-GunYu`是一款redis数据治理工具，可以进行数据实时同步，数据迁移，备份、校验恢复，数据分析等等。




## 特性



### 数据实时同步

`Redis-GunYu`的实时同步功能矩阵

|  功能点  |  是否支持  |
| :- | :- |
|  断点续传  |  支持  | 
|  源和目标集群slot不一致  |  支持  | 
|  源或目标集群拓扑变化(扩容、迁移等)  |  支持  | 
|  工具高可用  |  支持  | 
|  数据过滤 |  支持  |
|  数据一致性  |  最终/弱  | 


`redis-GunYu`还有一些其他优势，如下
- 对稳定性影响更小
  - 复制优先级：可用指定优先从从库进行复制或主库复制
  - 本地缓存 + 断点续传：最大程度减少对源端redis的影响
  - 对RDB中的大key进行拆分同步
  - 更低的复制延迟：在保证一致性的前提下，并发地进行数据回放，参考[复制延迟指标](docs/deployment_zh.md#监控)
- 数据安全性与高可用
  - 本地缓存支持数据校验
  - 工具高可用 ： 支持主从模式，以最新记录进行自主选举，自动和手动failover；工具本身P2P架构，将宕机影响降低到最小
- 对redis限制更少
  - 支持源和目标端不同的redis部署方式，如cluster或单实例
  - 兼容源和目的redis不同版本，支持从redis4.0到redis7.2，参考[测试](docs/test_zh.md#版本兼容测试)
- 数据一致性策略更加灵活，自动切换
  - 当源端和目标端分片信息一致时，采用伪事务方式批量写入，实时更新偏移，最大可能保证一致性
  - 当源端和目标端分片不一致时，采用定期更新偏移
- 运维更加友好
  - API：可以通过http API进行运维操作，如强制全量复制，同步状态，暂停同步等等
  - 监控：监控指标更丰富，如时间与空间维度的复制延迟指标
  - 数据过滤：可以对某些正则key，db，命令等进行过滤
  - 拓扑变化监控 ： 实时监听源和目标端redis拓扑变更（如加减节点，主从切换等等），以更改一致性策略和调整其他功能策略



### 其他

其他功能，仍在开发中。



## 产品比较

从功能点上对几个热门工具进行比较

功能点 | redis-shake/v2 |  DTS | xpipe | redis-GunYu
-- | -- | -- | -- | -- 
断点续传 | Y(无本地缓存)  | Y | Y | Y
支持分片不对称 | N | Y | N | Y
拓扑变化 | N |  N | N | Y
高可用 | N |  N | Y | Y
数据一致性 | 最终 |  弱 | 弱 | 最终(分片对称) + 弱(不对称)




## 实现

`redis-GunYu`的技术实现如图所示，具体技术原理请见[技术实现](docs/tech.md)

<img src="docs/imgs/sync.png" width = "400" height = "150" alt="架构图" align=center />



## 快速开始

### 安装

**下载二进制**



**编译源码**

先确保已经安装Go语言，配置好环境变量

```
git clone https://github.com/mgtv-tech/redis-gunyu.git
cd redis-GunYu

## 如果需要，添加代理
export GOPROXY=https://goproxy.cn,direct

make
```
在本地生成`redisGunYu`二进制文件。


### 使用

```
./redisGunYu -conf ./config.yaml
```


## 文档

- [配置](docs/configuration_zh.md)
- [部署](docs/deployment_zh.md)
- [API](docs/API_zh.md)
- [测试结果](docs/test_zh.md)
- [注意事项](docs/attentions_zh.md)




## 贡献

欢迎大家一起来完善redis-GunYu。如果您有任何疑问、建议或者想添加其他功能，请直接提交issue或者PR。

请按照以下步骤来提交PR：
- 克隆仓库
- 创建一个新分支：如果是新功能分支，则命名为feature-xxx；如果是修复bug，则命名为bug-xxx
- 在PR中详细描述更改的内容


## 许可

`redis-GunYu`是基于Apache2.0许可的，请见[LICENSE](LICENSE)。


## 联系

如果您有任何问题，请联系`ikenchina@gmail.com`。
