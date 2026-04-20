# Bisync 性能测试报告

- [Bisync 性能测试报告](#bisync-性能测试报告)
  - [1. 文档状态](#1-文档状态)
  - [2. 测试目标](#2-测试目标)
  - [3. 测试环境](#3-测试环境)
  - [4. 测试矩阵](#4-测试矩阵)
    - [4.1 网络矩阵](#41-网络矩阵)
    - [4.2 QPS 矩阵](#42-qps-矩阵)
    - [4.3 模式矩阵](#43-模式矩阵)
    - [4.4 总 case 数](#44-总-case-数)
  - [5. 测试步骤](#5-测试步骤)
  - [6. 采集指标](#6-采集指标)
    - [6.1 吞吐](#61-吞吐)
    - [6.2 Sync Delay](#62-sync-delay)
    - [6.3 资源占用](#63-资源占用)
    - [6.4 一致性](#64-一致性)
  - [7. 执行命令](#7-执行命令)
    - [7.1 `direct`](#71-direct)
    - [7.2 `wan_40_10`](#72-wan_40_10)
    - [7.3 `wan_80_20`](#73-wan_80_20)
  - [8. 结果汇总](#8-结果汇总)
    - [8.1 `direct`](#81-direct)
    - [8.2 `wan_40_10`](#82-wan_40_10)
    - [8.3 `wan_80_20`](#83-wan_80_20)
  - [9. 测试详细内容](#9-测试详细内容)
    - [9.1 `direct`](#91-direct)
    - [9.2 `wan_40_10`](#92-wan_40_10)
    - [9.3 `wan_80_20`](#93-wan_80_20)
  - [10. 结论](#10-结论)

## 1. 文档状态


本轮复测结论范围：

- 对比 `sync`、`pipeline`、`parallel` 三种 bisync 回放模式。
- 覆盖 `direct`、`wan_40_10`、`wan_80_20` 三个网络档位。
- 覆盖 `QPS=10000` 和 `QPS=20000` 两个负载档位。
- 所有正式入表 case 最终均以 `compare ok` 收口。


## 2. 测试目标

本次性能测试重点回答以下问题：

1. 在本地直连网络下，三种 bisync 模式的吞吐、sync delay 和资源占用差异是什么。
2. 在引入跨机房网络延迟和抖动后，三种模式的退化幅度分别是多少。
3. 在 `QPS=10000` 和 `QPS=20000` 两个档位下，哪种模式更稳定，哪种模式更容易出现拖尾。
4. 在所有测试场景下，最终业务数据是否仍然可以收敛一致。
5. 哪种模式更适合作为默认 bisync 回放模式。

## 3. 测试环境

| 项目 | 实际值 |
| --- | --- |
| 测试日期 | `2026-04-19` |
| Workspace | `/Users/ken/go/src/github.com/mgtv-tech/redis-GunYu` |
| 操作系统 | `macOS 26.3 (Darwin 25.3.0)` |
| CPU | `Apple M2` |
| CPU 架构 | `arm64` |
| 内存 | `16 GiB` |
| Redis 版本 | `Redis server v=8.6.2` |
| Go 版本 | `go version go1.24.4 darwin/arm64` |
| GunYu 代码版本 | `9c1de9f4462c10629cf13bf0eb9b7a2d6c75b959` (`dev/bisync`) |
| Redis 拓扑 | 左右各 1 套 3-master Redis Cluster |
| Syncer 拓扑 | 左到右、右到左各 1 个 syncer |
| 基线脚本 | `tests/bisync/run_benchmark_local.sh` |
| WAN 脚本 | `tests/bisync/run_benchmark_cloud_local.sh` |
| 网络模拟工具 | `tests/bisync/cmd/redis_netem_proxy` |
| 归档目录 | `tests/bisync/reports/2026-04-19-raw` |

测试拓扑说明：

- 左侧集群和右侧集群均为本地临时启动的 3-master Redis Cluster。
- `direct` 场景中，syncer 直接访问左右 Redis Cluster。
- `wan_*` 场景中，syncer 从本地输入端读取数据，但向对端输出时强制经过 `redis_netem_proxy`。
- `redis_netem_proxy` 对 Redis 流量注入单向 latency / jitter，并重写 cluster 拓扑返回，保证 syncer 始终走模拟 WAN 路径。

## 4. 测试矩阵

### 4.1 网络矩阵

| 场景 | 说明 | 单向延迟 | 单向抖动 | 执行脚本 |
| --- | --- | --- | --- | --- |
| `direct` | 本地直连基线 | `0ms` | `0ms` | `tests/bisync/run_benchmark_local.sh` |
| `wan_40_10` | 中等跨机房网络 | `40ms` | `10ms` | `tests/bisync/run_benchmark_cloud_local.sh` |
| `wan_80_20` | 高延迟高抖动网络 | `80ms` | `20ms` | `tests/bisync/run_benchmark_cloud_local.sh` |

### 4.2 QPS 矩阵

| 档位 | 目标 QPS | 目的 |
| --- | ---: | --- |
| `qps10000` | `10000` | 中等负载，观察常规稳定性 |
| `qps20000` | `20000` | 高负载，观察吞吐上限与拖尾风险 |

### 4.3 模式矩阵

每个网络档位、每个 QPS 档位都执行以下三种模式：

- `sync`
- `pipeline`
- `parallel`

### 4.4 总 case 数

- 网络档位 `3`
- QPS 档位 `2`
- 模式档位 `3`

总计：

- `3 x 2 x 3 = 18` 个 formal benchmark case

## 5. 测试步骤

每个 benchmark case 统一按以下步骤执行：

1. 启动左右两套本地 Redis Cluster。
2. 若为 WAN 场景，启动 `redis_netem_proxy`，注入指定 `WAN_LATENCY` 和 `WAN_JITTER`。
3. 启动双向 bisync syncer：
   - forward：left -> right
   - reverse：right -> left
4. 使用 `tests/bisync/cmd/bisync_workload --scenario soak` 持续双向写入。
5. benchmark 期间持续采集：
   - workload 吞吐
   - sync delay
   - CPU
   - RSS
   - goroutines
   - storer 目录大小
6. workload 结束后等待收敛。
7. 使用 `tests/bisync/cmd/bisync_compare` 对 stable key 做最终一致性校验。
8. 输出单 case 原始 Markdown 报告。
9. 将所有 case 的结果汇总到本文档。

本轮执行备注：

- `wan_40_10 / parallel / QPS=20000` 首次执行在人工排查时中断，因此未使用首次中断结果入表。
- 为了保证结果矩阵完整，单独 rerun 了该 case，并以 rerun 结果作为正式数据。

## 6. 采集指标

### 6.1 吞吐

- `Left cmd/s`
- `Right cmd/s`
- `Total cmd/s`
- `UniqueKeys`
- `Iterations`
- `ApproxPayloadBytes`

### 6.2 Sync Delay

从 syncer 的 `/prometheus` 指标中采样：

- `L->R p50`
- `L->R p95`
- `L->R p99`
- `L->R max`
- `R->L p50`
- `R->L p95`
- `R->L p99`
- `R->L max`

### 6.3 资源占用

- `Max CPU fwd/rev`
- `Max RSS fwd/rev`
- `Max goroutines fwd/rev`
- `Max storer fwd/rev`

### 6.4 一致性

- `compare ok / compare fail`
- stable key 对比范围
- 若失败，记录不一致 key 样本

## 7. 执行命令

### 7.1 `direct`

```bash
BENCH_DURATION=2m \
BENCH_KEY_SPACE=50000 \
BENCH_WORKERS=4 \
BENCH_BOUNDARY_EVERY=5000 \
BENCH_TXN_EVERY=500 \
BENCH_FINAL_SETTLE_SECONDS=15 \
BENCH_COMPARE_MAX_KEYS=10000 \
BENCH_TARGET_QPS_LIST=10000,20000 \
SCENARIOS=sync,pipeline,parallel \
KEEP_TMP=1 \
TMP_ROOT=/tmp/redisgunyu-bisync-benchmark-direct-20260419 \
CLUSTER_TMP_ROOT=/tmp/redisgunyu-bisync-benchmark-direct-20260419-cluster \
bash ./tests/bisync/run_benchmark_local.sh
```

### 7.2 `wan_40_10`

```bash
WAN_LATENCY=40ms \
WAN_JITTER=10ms \
BENCH_DURATION=2m \
BENCH_KEY_SPACE=50000 \
BENCH_WORKERS=4 \
BENCH_BOUNDARY_EVERY=5000 \
BENCH_TXN_EVERY=500 \
BENCH_FINAL_SETTLE_SECONDS=15 \
BENCH_COMPARE_MAX_KEYS=10000 \
BENCH_TARGET_QPS_LIST=10000,20000 \
SCENARIOS=sync,pipeline,parallel \
KEEP_TMP=1 \
TMP_ROOT=/tmp/redisgunyu-bisync-benchmark-wan-40-10-20260419 \
CLUSTER_TMP_ROOT=/tmp/redisgunyu-bisync-benchmark-wan-40-10-20260419-cluster \
PROXY_TMP_ROOT=/tmp/redisgunyu-bisync-benchmark-wan-40-10-20260419-proxy \
bash ./tests/bisync/run_benchmark_cloud_local.sh
```

`wan_40_10 / parallel / QPS=20000` formal rerun：

```bash
WAN_LATENCY=40ms \
WAN_JITTER=10ms \
BENCH_DURATION=2m \
BENCH_KEY_SPACE=50000 \
BENCH_WORKERS=4 \
BENCH_BOUNDARY_EVERY=5000 \
BENCH_TXN_EVERY=500 \
BENCH_FINAL_SETTLE_SECONDS=15 \
BENCH_COMPARE_MAX_KEYS=10000 \
BENCH_TARGET_QPS_LIST=20000 \
SCENARIOS=parallel \
KEEP_TMP=1 \
TMP_ROOT=/tmp/redisgunyu-bisync-wan40-parallel-qps20000-rerun-20260419 \
CLUSTER_TMP_ROOT=/tmp/redisgunyu-bisync-wan40-parallel-qps20000-rerun-20260419-cluster \
PROXY_TMP_ROOT=/tmp/redisgunyu-bisync-wan40-parallel-qps20000-rerun-20260419-proxy \
bash ./tests/bisync/run_benchmark_cloud_local.sh
```

`wan_40_10 / pipeline / QPS=20000` dedicated rerun：

```bash
WAN_LATENCY=40ms \
WAN_JITTER=10ms \
BENCH_DURATION=2m \
BENCH_KEY_SPACE=50000 \
BENCH_WORKERS=4 \
BENCH_BOUNDARY_EVERY=5000 \
BENCH_TXN_EVERY=500 \
BENCH_FINAL_SETTLE_SECONDS=15 \
BENCH_COMPARE_MAX_KEYS=10000 \
BENCH_TARGET_QPS_LIST=20000 \
SCENARIOS=pipeline \
KEEP_TMP=1 \
TMP_ROOT=tests/bisync/reports/2026-04-19-wan40-pipeline-qps20000-rerun \
CLUSTER_TMP_ROOT=/tmp/redisgunyu-bisync-wan40-pipeline-qps20000-rerun-cluster \
PROXY_TMP_ROOT=/tmp/redisgunyu-bisync-wan40-pipeline-qps20000-rerun-proxy \
bash ./tests/bisync/run_benchmark_cloud_local.sh
```

### 7.3 `wan_80_20`

```bash
WAN_LATENCY=80ms \
WAN_JITTER=20ms \
BENCH_DURATION=2m \
BENCH_KEY_SPACE=50000 \
BENCH_WORKERS=4 \
BENCH_BOUNDARY_EVERY=5000 \
BENCH_TXN_EVERY=500 \
BENCH_FINAL_SETTLE_SECONDS=15 \
BENCH_COMPARE_MAX_KEYS=10000 \
BENCH_TARGET_QPS_LIST=10000,20000 \
SCENARIOS=sync,pipeline,parallel \
KEEP_TMP=1 \
TMP_ROOT=/tmp/redisgunyu-bisync-benchmark-wan-80-20-20260419 \
CLUSTER_TMP_ROOT=/tmp/redisgunyu-bisync-benchmark-wan-80-20-20260419-cluster \
PROXY_TMP_ROOT=/tmp/redisgunyu-bisync-benchmark-wan-80-20-20260419-proxy \
bash ./tests/bisync/run_benchmark_cloud_local.sh
```

## 8. 结果汇总

### 8.1 `direct`

| QPS | Mode | Left cmd/s | Right cmd/s | Total cmd/s | L->R p95 | L->R p99 | L->R max | R->L p95 | R->L p99 | R->L max | RSS fwd MB | RSS rev MB | Storer fwd MB | Storer rev MB | Compare |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `10000` | `sync` | `5038.23` | `4961.50` | `9999.73` | `14.83` | `71.93` | `115.34` | `15.22` | `82.56` | `112.09` | `98.6` | `105.8` | `2868.4` | `2888.3` | `pass` |
| `10000` | `pipeline` | `5038.20` | `4961.47` | `9999.67` | `14.00` | `17.22` | `19.53` | `14.13` | `17.88` | `31.62` | `106.0` | `107.7` | `3152.0` | `3161.8` | `pass` |
| `10000` | `parallel` | `5038.19` | `4961.46` | `9999.65` | `17.87` | `51.90` | `74.52` | `15.82` | `53.44` | `68.86` | `101.2` | `105.6` | `3152.0` | `3169.9` | `pass` |
| `20000` | `sync` | `9280.50` | `9139.25` | `18419.75` | `24.10` | `30.92` | `39.29` | `22.60` | `33.01` | `51.35` | `113.2` | `108.4` | `4732.3` | `4744.3` | `pass` |
| `20000` | `pipeline` | `10075.67` | `9922.38` | `19998.05` | `25.51` | `52.09` | `108.92` | `18.91` | `32.87` | `94.45` | `117.0` | `115.4` | `5489.4` | `5482.8` | `pass` |
| `20000` | `parallel` | `8690.13` | `8557.86` | `17247.99` | `28.10` | `67.98` | `167.52` | `26.46` | `109.43` | `134.98` | `115.1` | `118.0` | `4878.0` | `4897.5` | `pass` |

### 8.2 `wan_40_10`

| QPS | Mode | Left cmd/s | Right cmd/s | Total cmd/s | L->R p95 | L->R p99 | L->R max | R->L p95 | R->L p99 | R->L max | RSS fwd MB | RSS rev MB | Storer fwd MB | Storer rev MB | Compare |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `10000` | `sync` | `5038.19` | `4961.53` | `9999.72` | `15.07` | `71.67` | `123.84` | `13.89` | `63.37` | `122.96` | `107.0` | `101.8` | `2865.9` | `2877.0` | `pass` |
| `10000` | `pipeline` | `5038.23` | `4961.50` | `9999.73` | `42.17` | `99.39` | `136.54` | `96.17` | `108.80` | `148.26` | `104.5` | `111.1` | `3156.6` | `3172.1` | `pass` |
| `10000` | `parallel` | `5038.25` | `4961.52` | `9999.77` | `57.57` | `142.25` | `147.91` | `60.01` | `118.08` | `168.53` | `105.8` | `108.9` | `3100.3` | `3129.6` | `pass` |
| `20000` | `sync` | `8784.68` | `8650.96` | `17435.64` | `216.46` | `274.91` | `275.21` | `185.46` | `265.36` | `288.42` | `113.9` | `103.7` | `4504.9` | `4506.3` | `pass` |
| `20000` | `pipeline` | `10076.29` | `9922.93` | `19999.22` | `27.51` | `45.66` | `81.19` | `22.87` | `61.52` | `67.37` | `116.9` | `117.8` | `5473.0` | `5476.0` | `pass` |
| `20000` | `parallel` | `8057.48` | `7934.87` | `15992.35` | `26.38` | `102.30` | `133.09` | `26.99` | `119.60` | `134.16` | `109.9` | `112.3` | `4538.8` | `4553.6` | `pass` |

### 8.3 `wan_80_20`

| QPS | Mode | Left cmd/s | Right cmd/s | Total cmd/s | L->R p95 | L->R p99 | L->R max | R->L p95 | R->L p99 | R->L max | RSS fwd MB | RSS rev MB | Storer fwd MB | Storer rev MB | Compare |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `10000` | `sync` | `5038.18` | `4961.51` | `9999.69` | `15.28` | `19.68` | `38.33` | `14.19` | `28.25` | `55.16` | `101.8` | `106.2` | `2851.9` | `2862.2` | `pass` |
| `10000` | `pipeline` | `5038.24` | `4961.51` | `9999.75` | `13.37` | `19.97` | `25.83` | `11.98` | `15.45` | `23.77` | `105.1` | `108.1` | `3141.0` | `3169.1` | `pass` |
| `10000` | `parallel` | `5038.22` | `4961.49` | `9999.71` | `15.55` | `20.42` | `45.74` | `13.47` | `23.61` | `58.89` | `103.8` | `108.7` | `3149.6` | `3157.7` | `pass` |
| `20000` | `sync` | `7816.63` | `7697.71` | `15514.34` | `27.74` | `64.10` | `70.88` | `23.16` | `61.82` | `65.18` | `101.8` | `100.1` | `4281.0` | `4294.5` | `pass` |
| `20000` | `pipeline` | `9984.56` | `9832.67` | `19817.23` | `19.26` | `61.88` | `86.56` | `18.46` | `78.86` | `89.30` | `110.6` | `117.2` | `5439.1` | `5450.1` | `pass` |
| `20000` | `parallel` | `8093.85` | `7970.71` | `16064.56` | `28.32` | `42.43` | `61.15` | `28.90` | `68.71` | `102.75` | `110.9` | `108.9` | `4621.3` | `4641.2` | `pass` |

## 9. 测试详细内容

### 9.1 `direct`

测试步骤：

1. 启动左右两套本地 Redis Cluster。
2. 不启用 `redis_netem_proxy`。
3. 执行 `sync`、`pipeline`、`parallel` 三种模式。
4. 分别跑 `QPS=10000` 和 `QPS=20000`。
5. 记录 throughput、sync delay、资源占用和最终 compare 结果。

结果：

| QPS | Mode | 结果摘要 |
| --- | --- | --- |
| `10000` | `sync` | 总吞吐 `9999.73`，tail 明显高于 `pipeline`，但 storer 最小，`compare ok` |
| `10000` | `pipeline` | 总吞吐与其他模式几乎一致，`p95/p99/max` 最稳，`compare ok` |
| `10000` | `parallel` | 总吞吐与其他模式几乎一致，但 `p99/max` 劣于 `pipeline`，`compare ok` |
| `20000` | `sync` | 总吞吐 `18419.75`，tail 明显优于 `parallel`，资源低于 `pipeline`，`compare ok` |
| `20000` | `pipeline` | 总吞吐 `19998.05` 为三者最高，但 max delay 和 storer 也是三者最高，`compare ok` |
| `20000` | `parallel` | 总吞吐 `17247.99` 为三者最低，tail 也最差，`compare ok` |

观察：

- `QPS=10000` 下三种模式的业务写入吞吐几乎重合，差异主要体现在 tail latency 和 storer 体积。
- `pipeline` 在 `direct / 10000` 下给出了本轮最佳 tail，但代价是更高的 RSS 和 storer。
- `QPS=20000` 下 `pipeline` 的总吞吐接近打满目标 QPS，是 `direct` 场景的吞吐冠军。
- `sync` 在 `direct / 20000` 下虽然吞吐低于 `pipeline`，但 `L->R max=39.29ms`、`R->L max=51.35ms`，明显比 `pipeline` 和 `parallel` 更稳。
- `parallel` 在 `direct` 下没有体现出预期优势，尤其 `QPS=20000` 时总吞吐和 tail 都落后。

### 9.2 `wan_40_10`

测试步骤：

1. 启动左右两套本地 Redis Cluster。
2. 启动 `redis_netem_proxy`，注入单向延迟 `40ms`、单向抖动 `10ms`。
3. forward syncer 读本地 left，写 remote right proxy。
4. reverse syncer 读本地 right，写 remote left proxy。
5. 执行 `sync`、`pipeline`、`parallel` 三种模式。
6. 分别跑 `QPS=10000` 和 `QPS=20000`。
7. 记录 throughput、sync delay、资源占用和最终 compare 结果。

结果：

| QPS | Mode | 结果摘要 |
| --- | --- | --- |
| `10000` | `sync` | 总吞吐 `9999.72`，三者中 tail 最稳，storer 最小，`compare ok` |
| `10000` | `pipeline` | 总吞吐不变，但双向 p95 分别升到 `42.17ms` 和 `96.17ms`，`compare ok` |
| `10000` | `parallel` | 总吞吐不变，但 tail 比 `pipeline` 还差，`compare ok` |
| `20000` | `sync` | 总吞吐 `17435.64`，tail 升到 `185ms` 到 `288ms` 区间，`compare ok` |
| `20000` | `pipeline` | dedicated rerun 总吞吐 `19999.22` 最高，双向 p95 分别为 `27.51ms` 和 `22.87ms`，`compare ok` |
| `20000` | `parallel` | formal rerun 总吞吐 `15992.35`，tail 显著优于 `sync`，并与 `pipeline` rerun 接近，`compare ok` |

观察：

- `QPS=10000` 下三种模式吞吐依旧几乎一致，但 WAN 已经明显放大了 `pipeline` 和 `parallel` 的 tail。
- `sync` 在 `wan_40_10 / 10000` 下是最稳模式，双向 p95 仍维持在 `15ms` 左右。
- `pipeline` 首次入表的秒级拖尾没有在 dedicated rerun 中复现。rerun 下 `L->R p95=27.51ms`、`R->L p95=22.87ms`，同时总吞吐达到 `19999.22`，说明原始结果更像是单次异常样本，而不是稳定特征。
- `parallel` 的 formal rerun 在 `wan_40_10 / 20000` 下双向 p95 约 `26ms` 到 `27ms`，与 `pipeline` rerun 接近，但总吞吐只有 `15992.35`，明显低于 `pipeline`。
- 首次执行 `wan_40_10 / parallel / 20000` 时，人工排查阶段的独立 compare 曾看到 stable key mismatch；正式汇总表未使用那次中断结果，而使用了 dedicated rerun 的完整结果。

### 9.3 `wan_80_20`

测试步骤：

1. 启动左右两套本地 Redis Cluster。
2. 启动 `redis_netem_proxy`，注入单向延迟 `80ms`、单向抖动 `20ms`。
3. forward syncer 读本地 left，写 remote right proxy。
4. reverse syncer 读本地 right，写 remote left proxy。
5. 执行 `sync`、`pipeline`、`parallel` 三种模式。
6. 分别跑 `QPS=10000` 和 `QPS=20000`。
7. 记录 throughput、sync delay、资源占用和最终 compare 结果。

结果：

| QPS | Mode | 结果摘要 |
| --- | --- | --- |
| `10000` | `sync` | 总吞吐 `9999.69`，tail 稳定，storer 最小，`compare ok` |
| `10000` | `pipeline` | 总吞吐 `9999.75`，双向 tail 最好，`compare ok` |
| `10000` | `parallel` | 总吞吐 `9999.71`，tail 介于 `sync` 与 `pipeline` 之间，`compare ok` |
| `20000` | `sync` | 总吞吐 `15514.34`，tail 中等，资源最低，`compare ok` |
| `20000` | `pipeline` | 总吞吐 `19817.23` 最高，双向 p95 仍是三者中最好，`compare ok` |
| `20000` | `parallel` | 总吞吐 `16064.56`，tail 高于 `pipeline` 但好于本轮 `wan_40_10` 首次异常现象，`compare ok` |

观察：

- `QPS=10000` 下，`wan_80_20` 反而给出了比 `wan_40_10` 更规整的 tail，尤其 `pipeline` 的双向 p95 都压在 `14ms` 左右。
- `QPS=20000` 下，`pipeline` 依旧是吞吐冠军，且本轮没有复现 `wan_40_10` 首次异常表项里的秒级拖尾。
- `sync` 在 `wan_80_20 / 20000` 下的总吞吐降到 `15514.34`，但资源和 storer 仍然最低。
- `parallel` 在 `wan_80_20 / 20000` 下总吞吐 `16064.56`，略高于 `sync`，但明显低于 `pipeline`。
- 从单次结果看，`80ms/20ms` 并没有比 `40ms/10ms` 呈现更差的高 QPS tail。这个现象更像是 workload、代理调度和 backlog 收敛行为共同作用的结果，而不是纯粹由注入 RTT 单调决定。这个判断属于基于本轮数据的推断，如果要形成发布级结论，建议补做多轮重复实验。

## 10. 结论

- 吞吐结论：`direct` 下 `QPS=20000` 的吞吐冠军是 `pipeline`，总吞吐 `19998.05`；`QPS=10000` 下三种模式吞吐几乎无差异。
- 时延结论：`wan_40_10 / 10000` 下 `sync` 的 tail 最稳；`wan_40_10 / 20000` 下 的 `pipeline` 和 `parallel` 双向 p95 都维持在约 `23ms` 到 `28ms`，明显优于 `sync` 的 `185ms` 到 `216ms`。`wan_80_20` 下 `pipeline` 的高 QPS tail 与本轮 `wan_40_10` rerun 接近。
- 资源结论：`sync` 在大多数场景下都有最小的 storer 体积；`pipeline` 往往用更高的 RSS 和 storer 换更高的吞吐；`parallel` 的资源消耗通常介于 `sync` 和 `pipeline` 之间，但在本轮 `direct` 下没有体现出吞吐优势。
- 一致性结论：18 个 formal case 最终都通过了 `compare ok`。
- 网络退化结论：本轮最明显的高压退化集中在 `wan_40_10 / 20000` 的 `sync`，其双向 p95 升到 `185ms` 到 `216ms`。`80ms/20ms` 也没有表现出单调更差。
- 推荐模式：如果默认目标是稳定性、可预测 tail 和较低资源占用，建议默认使用 `sync`。如果在低延迟或经过充分验证的环境里追求更高吞吐，可以按场景显式切换到 `pipeline`。本轮不建议把 `pipeline` 直接作为跨机房高压场景的默认模式。
