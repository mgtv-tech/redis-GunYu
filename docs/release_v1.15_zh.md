# redis-GunYu v1.15 发布说明

## 1. 主要更新

- 修复 [#118](https://github.com/mgtv-tech/redis-GunYu/issues/118)。
- 新增 `server.initialPaused` 配置，以及运行期 `/syncer/pause` 和
  `/syncer/resume` 控制接口（[#119](https://github.com/mgtv-tech/redis-GunYu/issues/119)）。
- 补充生命周期、HA 切主、暂停与恢复的测试覆盖和文档。

## 2. 兼容性与升级说明

本版本为 `syncer.SyncerConfig` 新增了导出的 `InitialPaused` 字段。下游 Go
代码若使用无字段名的复合字面量构造 `SyncerConfig`，升级时必须改用具名字段
字面量。YAML 配置、CLI 配置和具名字段 Go 字面量保持兼容。

如果省略 `server.initialPaused` 或将其设为 `false`，同步会与此前版本一样自动
启动。

## 3. 启动时暂停与运行期控制

当 GunYu 进程需要先加入拓扑发现和 HA 选举、确认解析出的输入节点和选举角色，
或在受控切换前由运维人员确认时，可使用 `server.initialPaused: true`。启用后，
GunYu 在开始从源 Redis 读取或向目标 Redis 写入前保持暂停。

```yaml
server:
  listen: 127.0.0.1:18001
  initialPaused: true
```

按正常方式启动 GunYu，然后等待所有预期 input 在 `/syncer/status` 中显示
`State: "pause"`：

```bash
curl http://127.0.0.1:18001/syncer/status
```

此时 Redis 复制的输入和输出管线尚未启动；拓扑发现以及 HA 模式下的 leader
选举仍会运行。要启动本 GunYu 进程管理的全部管线，恢复所有 input：

```bash
curl -XPOST 'http://127.0.0.1:18001/syncer/resume?inputs=all'
```

如需只恢复一个 input，请先从 `/syncer/status` 获取其准确的 `Input` 值，再传给
`inputs`：

```bash
curl -XPOST 'http://127.0.0.1:18001/syncer/resume?inputs=127.0.0.1:16379'
```

在将切换视为生效前，请确认受影响管线的 `State` 已变为 `"run"`。`resume`
操作是幂等的，因此自动化程序可以安全重试。

已运行的指定管线可通过 `pause` 暂停：

```bash
curl -XPOST 'http://127.0.0.1:18001/syncer/pause?inputs=all'
```

当 `/syncer/restart` 重建同步器时，所选 input 的运行期状态会保留：暂停的
input 仍保持暂停，运行中的 input 会继续运行。

在 HA 模式下，需要在每个允许复制的 GunYu 进程上恢复相应 input；follower
等待成为 leader 时也可以处于暂停或运行状态。之后发生 leader 切换时会保留
该进程本地的状态。

`initialPaused` 只影响新初始化的 input。停止进程应使用 `DELETE /` 接口；新的
进程启动后，应通过 `/syncer/status` 确认当前状态。

## 4. 相关文档

- 配置说明：[sync_configuration_zh.md](./sync_configuration_zh.md)
- HTTP API 说明：[API_zh.md](./API_zh.md)
