# redis-GunYu v1.16 Release Notes

redis-GunYu v1.16 adds native Redis Sentinel support for one-way
synchronization, including source and target discovery, failover recovery,
independent authentication and TLS settings, and stable GunYu HA election
identity.

## 1. Highlights

- Added native Redis Sentinel discovery for both source and target Redis.
- Added independent authentication and TLS settings for Sentinel and Redis data
  nodes.
- Added Sentinel source selection through `master`, `slave`, and
  `prefer_slave`; Sentinel targets always resolve and write to the current
  master.
- Added topology monitoring and syncer reconstruction after source or target
  failover, reusing the existing replication ID, channel, and checkpoint
  recovery path.
- Added a stable Sentinel HA election identity based on `masterName`, so two
  GunYu instances continue to compete for the same logical source after a Redis
  master changes.
- Added real-Redis Sentinel failover, security, dual-GunYu HA, compatibility,
  and upgrade/rollback release gates.

## 2. Supported Topologies and Replay Modes

Sentinel may be used on either side of a one-way synchronization. The supported
source/target combinations are:

| Source | Target | Status |
| --- | --- | --- |
| Sentinel | Standalone | Supported |
| Standalone | Sentinel | Supported |
| Sentinel | Sentinel | Supported |
| Sentinel | Cluster | Supported |
| Cluster | Sentinel | Supported |

Sentinel topologies support `sync` and `pipeline` replay. Sentinel with
`bisyncEnabled: true` is rejected during configuration validation. The
`parallel` replay mode is bisync-only and is therefore not available with a
Sentinel source or target.

## 3. Configuration

For `type: sentinel`, `addresses` contains Sentinel endpoints, not Redis data
node endpoints. Top-level Redis credentials and `tlsEnable` apply to data
nodes; `sentinelOptions` applies only to Sentinel connections.

```yaml
input:
  redis:
    type: sentinel
    addresses:
      - 10.0.0.11:26379
      - 10.0.0.12:26379
      - 10.0.0.13:26379
    userName: data-user
    password: data-password
    tlsEnable: false
    sentinelOptions:
      masterName: source-redis
      userName: sentinel-user
      password: sentinel-password
      tlsEnable: false
  syncFrom: prefer_slave

output:
  redis:
    type: sentinel
    addresses:
      - 10.1.0.11:26379
      - 10.1.0.12:26379
      - 10.1.0.13:26379
    userName: data-user
    password: data-password
    tlsEnable: false
    sentinelOptions:
      masterName: target-redis
      userName: sentinel-user
      password: sentinel-password
      tlsEnable: false
  replay:
    mode: pipeline
```

`sentinelOptions.masterName` and at least one Sentinel address are required.
Configure multiple Sentinel addresses so discovery can continue when one
Sentinel is unavailable.

## 4. Failover and Consistency

GunYu periodically resolves Sentinel topology. When the selected source or
target master changes, it stops the affected syncer and rebuilds it with the
new physical address. Recovery attempts partial resynchronization through the
existing replication ID, local channel, and checkpoint. It falls back to a full
sync when partial resynchronization is not possible.

Redis Sentinel failover is based on asynchronous replication. Writes accepted
by the old master but not replicated to the promoted replica may be lost. This
release retains GunYu's eventual/weak consistency model and does not provide
zero RPO, exactly-once replay, or strong consistency. Validate business values
and checkpoint state after a failover, especially for non-idempotent commands.

## 5. Security and Deployment Requirements

- Sentinel and data-node ACL users may be different.
- Sentinel TLS and data-node TLS may be enabled independently.
- Sentinel-announced master and replica addresses must be directly reachable
  from GunYu. This release does not perform NAT or address rewriting.
- When Sentinel authenticates to data nodes with an ACL user, that user must
  have the command, key, and Pub/Sub channel permissions required by Sentinel,
  including access to the Sentinel hello channel.
- For TLS-only Sentinel deployments, configure reachable Sentinel
  `announce-ip` and `announce-port` values that identify the TLS endpoint.

## 6. GunYu HA Behavior

For a Sentinel source, GunYu HA uses the logical identity
`sentinel/<escaped-masterName>` instead of the current Redis master address.
The election key therefore remains stable across Redis failover. Qualified
tests verified that one GunYu process remained leader, the other remained
follower, and the follower took over after the leader stopped while writes
continued.

## 7. Compatibility and Qualification

- Redis 7.4.1 is the qualified release baseline.
- Redis 8.0.0 passed Sentinel core compatibility in `sync` and `pipeline`,
  including continuous writes and failover. Redis 8 is not durability-qualified
  until the required soak and benchmark gates have completed.
- Existing standalone and Cluster configurations remain compatible. The new
  Sentinel fields are optional unless `type: sentinel` is selected.
- This release does not change the checkpoint or persistent metadata schema.

The non-durability release qualification completed with:

- `make test-release`: passed, including static checks, all unit tests, the full
  race detector, required integration, and E2E smoke.
- Required integration: 241 tests, 0 failures, 0 skipped, 0 missing.
- E2E smoke: 7 of 7 cases passed.
- Coverage: 33.5%, above the 30.5% gate.
- Redis 7.4.1 Sentinel ACL/TLS matrix: all four permutations passed.
- Redis 7.4.1 Sentinel upgrade/rollback with the retained v1.14 binary: passed.
- Dual-GunYu Sentinel HA on Redis 7.4.1 and 8.0.0: `sync` and `pipeline` passed
  with continuous writes and exact source/target business values.

Performance, benchmark, and soak tests were intentionally excluded from this
qualification. A non-blocking observability issue remains: expected follower
startup or handover retries may log `empty run id`, connection-unavailable, or
reader errors at error level before convergence.

## 8. Upgrade and Rollback

To introduce Sentinel safely:

1. Back up the GunYu configuration, local channel directory, and target Redis
   checkpoint keys.
2. Upgrade the binary while retaining the existing direct-address
   configuration, then verify synchronization and checkpoint progress.
3. Stop every GunYu instance using the old direct-address election identity
   before enabling `type: sentinel` on any instance in the same HA group.
4. Start the Sentinel-configured instances, verify the resolved source and
   target masters in logs/status, and confirm that exactly one GunYu instance is
   leader for each logical source.
5. Resume writes and validate business values and checkpoints.

Do not run old direct-address HA instances concurrently with Sentinel-mode
instances for the same source. They use different election identities and are
not mutually exclusive.

For rollback to a pre-Sentinel release:

1. Ask Sentinel for the current source and target master addresses.
2. Stop all Sentinel-mode GunYu instances in the HA group.
3. Change both Redis configurations to `type: standalone`, replace `addresses`
   with the resolved data-node master addresses, and remove
   `sentinelOptions`.
4. Start the previous binary and verify business values and checkpoint
   progress before restoring normal traffic.

The qualified v1.14 rollback reused the existing channel and checkpoint without
a metadata migration.

## 9. Known Limitations

- Sentinel with bisync is not supported.
- Redis Cluster managed through Sentinel is not supported.
- Failover detection uses periodic topology resolution rather than Sentinel
  `+switch-master` Pub/Sub notifications.
- Sentinel-announced addresses are used as returned and must be reachable.
- Redis asynchronous failover may lose writes that were not replicated before
  promotion.

## 10. Related Documentation

- [Configuration guide](https://github.com/mgtv-tech/redis-GunYu/blob/v1.16/docs/sync_configuration_en.md)
- [Operational cautions](https://github.com/mgtv-tech/redis-GunYu/blob/v1.16/docs/attentions_en.md)
- [Test and compatibility status](https://github.com/mgtv-tech/redis-GunYu/blob/v1.16/docs/test_en.md)

**Full Changelog:** [v1.15...v1.16](https://github.com/mgtv-tech/redis-GunYu/compare/v1.15...v1.16)
