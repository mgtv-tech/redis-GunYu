# Redis-GunYu ClickHouse 审计落地说明

本文说明如何将 output 侧“过滤命令”和“发送命令”异步写入 ClickHouse，便于双向同步排障。

## 1. 能力说明

- 异步后台写入（不阻塞同步主流程）
- 批量写入（对 ClickHouse 友好）
- 两张表分离：
  - `sync_cmd_filtered`
  - `sync_cmd_sent`
- 表名支持自定义；未配置时自动回退默认表名

## 2. 配置示例

在同步配置中增加 `audit` 段：

```yaml
audit:
  enabled: true
  endpoint: "http://127.0.0.1:8123"
  database: "default"
  user: ""
  password: ""

  # 可选：不填使用默认值
  filteredTable: "sync_cmd_filtered"
  sentTable: "sync_cmd_sent"
  enableRecordFiltered: true        # 是否写 sync_cmd_filtered，默认 true（也可用 HTTP 动态改）

  queueSize: 50000
  batchSize: 1000
  flushInterval: 1s
  timeout: 3s
```

### 表名默认值

- `filteredTable` 为空 -> `sync_cmd_filtered`
- `sentTable` 为空 -> `sync_cmd_sent`
- `enableRecordFiltered` 为空 -> `true`

内部噪声（如 `script_keys` / `cmd_keys`、`script load`、`select 0` 等）**固定不落 ClickHouse**，仅依赖 metrics / 日志排查。

## 3. ClickHouse 建表 DDL

### 3.1 过滤命令表

```sql
CREATE TABLE IF NOT EXISTS sync_cmd_filtered
(
    dt         DateTime64(3),
    input      String,
    cmd        LowCardinality(String),
    key_value  String,                        -- cmd+key/value，最多保留 1KB
    reason     LowCardinality(String),
    stage      LowCardinality(String),        -- 见 §3.1.1，仅 parse_aof / txn_guard
    node       String
)
ENGINE = MergeTree
PARTITION BY toDate(dt)
ORDER BY (input, dt, cmd)
TTL toDate(dt) + INTERVAL 15 DAY
SETTINGS index_granularity = 8192;
```

### 3.1.1 `sync_cmd_filtered.stage`（与代码一致）

| 取值 | 含义 |
|------|------|
| `parse_aof` | AOF 增量解析路径上的过滤（黑名单、key 过滤、checkpoint 脚本 key、db/sentinel 等） |
| `txn_guard` | `MULTI…EXEC` 内因 checkpoint 规则整包丢弃时，仅落库事务内**业务**命令侧证据 |

**不包含 RDB**：全量 RDB 回放（`RESTORE`/key 流）**当前实现不写入** `sync_cmd_filtered` / `sync_cmd_sent`；排障以 AOF 命令审计为主，无需为 RDB 单独建 `stage`。

### 3.2 发送命令表

```sql
CREATE TABLE IF NOT EXISTS sync_cmd_sent
(
    dt         DateTime64(3),
    input      String,
    target     String,
    cmd        LowCardinality(String),
    key_value  String,                        -- cmd+key/value，最多保留 1KB
    node       String
)
ENGINE = MergeTree
PARTITION BY toDate(dt)
ORDER BY (input, target, dt, cmd)
TTL toDate(dt) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;
```

## 4. key_value 字段说明

- 来源：`cmd + args` 拼接文本
- 截断：最多保留 1KB，超出追加 `...(truncated)`
- 目的：控制写入体积，避免大 payload 影响 ClickHouse 写入稳定性

## 5. 运行参数建议

- `queueSize`: 50000
- `batchSize`: 1000（可按吞吐调到 2000/5000）
- `flushInterval`: 1s
- `timeout`: 3s

若写入压力很高，优先增大 `batchSize` 和 `queueSize`，并观察 ClickHouse 写入延迟与内存占用。

## 5.1 运行时 HTTP 开关（无需重启）

与 `printCmdToTarget` 类似，**只暴露一个**审计相关 HTTP 接口（查询 + 变更）：

- **查询状态**：`GET /syncer/audit`  
  返回 `auditEnabled`、`enableRecordFiltered`（生效值）、`enqueueAllowed`（进程内投递门闸是否打开）。
- **运行时修改**：`POST /syncer/audit`  
  至少指定下列**一个或两个**查询参数（`yes|no`，亦支持 `true|false`、`1|0`）：
  - `enable`：总开关，对应 `audit.enabled`，并驱动 ClickHouse 投递门闸；开启前需已在配置中填写有效 `audit.endpoint` 等，会走 `PrepareRuntime()`。
  - `enableRecordFiltered`：是否写入 `sync_cmd_filtered`，对应配置项同名；**不**影响 `sync_cmd_sent`，也**不**单独关闭总开关。  
  示例：只关 filtered 表 — `POST /syncer/audit?enableRecordFiltered=no`；同时开总审计且开 filtered — `POST /syncer/audit?enable=yes&enableRecordFiltered=yes`。

说明：若进程**首次启动时**未配置 `endpoint`，后续仅通过 HTTP 打开总开关时也必须先在配置里具备合法 `endpoint`（或 YAML 中已写好，仅 `enabled` 曾为 false），否则 `enable=yes` 会校验失败。

审计行中的 `node` 字段优先使用**本机非回环 IPv4** + HTTP 监听端口；若无可用 IPv4 则回退为 hostname。

## 6. 说明

- **审计范围**：仅覆盖 **AOF 命令流**（过滤表 + 成功发往目标端的命令）。**RDB 全量同步路径不落审计表**（与产品取舍一致，实现亦未接入）。
- 审计链路定位为“排障增强”，在极端压力下允许丢审计，不应反向阻塞主同步链路。
- **Prometheus 指标**（`subsystem=audit`，与 `config.AppName` 组合为完整指标名，一般为 `redisGunYu_audit_*`）：
  - `audit_enqueue_dropped_total{channel="filtered|sent"}`：内存队列满时非阻塞入队丢弃次数。
  - `audit_flush_failed_total{channel="filtered|sent"}`：写 ClickHouse 失败次数（当前实现不重试，对应批次直接丢弃）。
  - `audit_flush_failed_rows_total{channel="filtered|sent"}`：因 flush 失败而丢失的行数（与上一指标同一次失败对应）。
  排查：丢弃持续升高 → 增大 `queueSize` / `batchSize` 或降低审计采样；flush 失败 → 查 CH 可用性、网络、认证与表 DDL。
- 为降低写入量，以下内部保护/噪声类过滤 reason 默认不落 `sync_cmd_filtered`：
  - `script_keys`
  - `cmd_keys`
  - `sentinel_hello`
  这些事件仍可通过内置 metrics 观测。
- 例外：`stage=txn_guard` 的过滤事件默认保留，用于定位 `multi...exec`
  因 checkpoint 命中导致的整事务过滤；明细中默认仅保留事务内业务命令，
  不保留 checkpoint/internal 命令明细。
- `sync_cmd_sent` 固定不记录内部命令：`script load`、`select 0` 等。

## 7. 最小验收步骤

### Step 1：准备与启动

1. 在 ClickHouse 执行第 3 节两张建表 DDL。
2. 在 sync 配置中开启 `audit.enabled=true` 并填好 `endpoint/database`。
3. 启动 syncer，确认服务正常运行（`/syncer/status` 可访问）。

### Step 2：触发样本流量

1. 向源端写入一批普通业务命令（如 `set` / `hset`）。
2. 若是双向场景，触发少量会被过滤的 checkpoint 相关命令。
3. 观察 10~30 秒，等待后台批量 flush 落表。

### Step 3：查询验证（ClickHouse）

1) 过滤表最近写入：

```sql
SELECT dt, input, cmd, reason, stage, key_value
FROM sync_cmd_filtered
WHERE dt > now() - INTERVAL 5 MINUTE
ORDER BY dt DESC
LIMIT 50;
```

2) 发送表最近写入：

```sql
SELECT dt, input, target, cmd, key_value
FROM sync_cmd_sent
WHERE dt > now() - INTERVAL 5 MINUTE
ORDER BY dt DESC
LIMIT 50;
```

3) 按 input 对比过滤/发送量：

```sql
SELECT input, count() AS cnt
FROM sync_cmd_filtered
WHERE dt > now() - INTERVAL 5 MINUTE
GROUP BY input
ORDER BY cnt DESC;

SELECT input, count() AS cnt
FROM sync_cmd_sent
WHERE dt > now() - INTERVAL 5 MINUTE
GROUP BY input
ORDER BY cnt DESC;
```

### Step 4：通过标准（建议）

- `sync_cmd_sent` 中可以看到业务命令持续写入。
- 双向场景下，`sync_cmd_filtered` 有合理的过滤记录（含 `reason/stage`）。
- `key_value` 字段可读，且超长命令已按 1KB 规则截断。
