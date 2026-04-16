# Bisync 性能测试报告

## 结果


| 指标 | Sync | Pipeline | Parallel | 判断 |
| --- | ---: | ---: | ---: | --- |
| 结果 | Pass | Pass | Pass | 三者都通过 |
| 实际 combined cmd/s | 17514.15 | 18670.85 | 16939.74 | `pipeline` 最高 |
| Left cmd/s | 8824.21 | 9406.98 | 8534.79 | `pipeline` 最高 |
| Right cmd/s | 8689.94 | 9263.87 | 8404.95 | `pipeline` 最高 |
| L->R p50 | 11.21ms | 8.78ms | 11.58ms | `pipeline` 最低 |
| L->R p95 | 34.58ms | 16.97ms | 25.05ms | `pipeline` 最低 |
| L->R p99 | 43.01ms | 24.17ms | 45.69ms | `pipeline` 最低 |
| L->R max | 55.18ms | 40.39ms | 51.88ms | `pipeline` 最低 |
| R->L p50 | 11.17ms | 8.53ms | 10.66ms | `pipeline` 最低 |
| R->L p95 | 26.89ms | 24.39ms | 25.58ms | `pipeline` 略低 |
| R->L p99 | 50.50ms | 39.11ms | 36.61ms | `parallel` 最低 |
| R->L max | 67.45ms | 49.85ms | 39.65ms | `parallel` 最低 |
| Max CPU fwd/rev | 134.1% / 105.6% | 138.4% / 106.1% | 143.7% / 118.6% | `sync` 最省 CPU |
| Max RSS fwd/rev | 95.8MB / 100.5MB | 119.3MB / 118.0MB | 110.5MB / 106.6MB | `sync` 最低 |
| Max goroutines fwd/rev | 70 / 70 | 82 / 81 | 80 / 79 | `sync` 最少 |
| Max storer fwd/rev | 3.53GiB / 3.54GiB | 4.13GiB / 4.11GiB | 3.75GiB / 3.76GiB | `sync` 最省磁盘 |
| 一致性校验 | Pass, 10000 keys | Pass, 10000 keys | Pass, 10000 keys | 持平 |

综合判断：
- `pipeline` ：性能最好。
- `sync` 和其他模式性能差异不大，是因为本地测试主要瓶颈不在 RTT，而且 sync 本身已经按 replay unit 做了事务批量提交，同时还少了 frontier/journal/index 这层控制面成本。



## 测试环境与命令

| 项目 | 值 |
| --- | --- |
| 操作系统 | `Darwin 25.3.0 arm64` |
| Redis | `Redis server v=8.6.2` |
| 部署方式 | 本地两套 3-master Redis Cluster |
| GunYu 部署 | 左到右、右到左各 1 个 syncer |
| Workload | `tests/bisync/cmd/bisync_workload --scenario soak` |
| 每档持续时间 | `90s` |
| Target QPS | `20000` |
| Workers | `4` |
| KeySpace | `10000` |
| 一致性校验 | compare `10000` 个 stable key 样本 |

批量执行命令：

```bash
BENCH_DURATION=90s \
BENCH_KEY_SPACE=10000 \
BENCH_TARGET_QPS_LIST=20000 \
BENCH_COMPARE_MAX_KEYS=10000 \
SCENARIOS=sync,pipeline,parallel \
BENCH_WORKERS=4 \
KEEP_TMP=1 \
TMP_ROOT=/tmp/redisgunyu-bisync-benchmark-report-20000 \
CLUSTER_TMP_ROOT=/tmp/redisgunyu-bisync-benchmark-report-20000-cluster \
bash ./tests/bisync/run_benchmark_local.sh
```

`parallel` 单独补跑命令：

```bash
BENCH_DURATION=90s \
BENCH_KEY_SPACE=10000 \
BENCH_TARGET_QPS_LIST=20000 \
BENCH_COMPARE_MAX_KEYS=10000 \
SCENARIOS=parallel \
BENCH_WORKERS=4 \
KEEP_TMP=1 \
TMP_ROOT=/tmp/redisgunyu-bisync-benchmark-report-20000-parallel \
CLUSTER_TMP_ROOT=/tmp/redisgunyu-bisync-benchmark-report-20000-parallel-cluster \
bash ./tests/bisync/run_benchmark_local.sh
```

`pipeline` 独立复测命令：

```bash
BENCH_DURATION=90s \
BENCH_KEY_SPACE=10000 \
BENCH_TARGET_QPS_LIST=20000 \
BENCH_COMPARE_MAX_KEYS=10000 \
SCENARIOS=pipeline \
BENCH_WORKERS=4 \
KEEP_TMP=1 \
TMP_ROOT=/tmp/redisgunyu-bisync-benchmark-report-pipeline-20000-rerun \
CLUSTER_TMP_ROOT=/tmp/redisgunyu-bisync-benchmark-report-pipeline-20000-rerun-cluster \
bash ./tests/bisync/run_benchmark_local.sh
```


