# redis-GunYu v1.15 Release Notes

## 1. Highlights

- Fixed [#118](https://github.com/mgtv-tech/redis-GunYu/issues/118).
- Added `server.initialPaused` and runtime `/syncer/pause` and
  `/syncer/resume` controls ([#119](https://github.com/mgtv-tech/redis-GunYu/issues/119)).
- Expanded lifecycle, HA failover, and pause/resume test coverage and
  documentation.

## 2. Compatibility and Upgrade Notes

This release adds the exported `InitialPaused` field to `syncer.SyncerConfig`.
Downstream Go code that constructs `SyncerConfig` using an unkeyed composite
literal must switch to a keyed literal when upgrading. YAML configuration, CLI
configuration, and keyed Go literals remain compatible.

If `server.initialPaused` is omitted or set to `false`, synchronization starts
automatically as in previous releases.

## 3. Paused Startup and Runtime Control

Use `server.initialPaused: true` when a GunYu process must join topology
discovery and HA election before it begins to read from the source Redis or
write to the target Redis. This is useful when an operator needs to inspect
the resolved inputs, confirm the elected roles, or coordinate a controlled
cutover before replication starts.

```yaml
server:
  listen: 127.0.0.1:18001
  initialPaused: true
```

Start GunYu normally, then wait for every expected input to appear in
`/syncer/status` with `State: "pause"`:

```bash
curl http://127.0.0.1:18001/syncer/status
```

At this point, Redis replication input and output pipelines have not started.
Topology discovery and, in HA mode, leader election continue to run. To start
all pipelines managed by this GunYu process, resume every input:

```bash
curl -XPOST 'http://127.0.0.1:18001/syncer/resume?inputs=all'
```

For a selected input, obtain its exact `Input` value from `/syncer/status` and
pass it to `inputs` instead:

```bash
curl -XPOST 'http://127.0.0.1:18001/syncer/resume?inputs=127.0.0.1:16379'
```

Verify that affected pipelines report `State: "run"` before treating the
cutover as active. `resume` is idempotent, so automation can safely retry it.

To temporarily stop selected pipelines after they have started:

```bash
curl -XPOST 'http://127.0.0.1:18001/syncer/pause?inputs=all'
```

The selected runtime state is retained when a syncer is recreated by
`/syncer/restart`: a paused input remains paused, and a running input resumes
running.

In HA mode, resume the inputs on each GunYu process that should be allowed to
replicate. A follower may be paused or running while it waits for leadership.
A later leader handover preserves that process-local state.

`initialPaused` affects only newly initialized inputs. Use `DELETE /` to stop
the process; after a new process starts, check `/syncer/status` to determine
its current state.

## 4. Related Documentation

- Configuration guide: [sync_configuration_en.md](./sync_configuration_en.md)
- HTTP API guide: [API_en.md](./API_en.md)
