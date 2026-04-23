# redis-GunYu v1.1.1 Release Notes

## 1. Overview

`v1.1.1` is a patch release on top of `v1.1.0`, focused on Redis reply error detection, Redis role detection accuracy, and stronger release validation coverage.

This version does not introduce new feature switches. The main goal is to improve observability and reliability in failure scenarios, while increasing confidence in pre-release validation. It is intended as a direct upgrade from `v1.1.0`.

## 2. Highlights

### 2.1 Stronger Redis Reply Error Detection

- Added `CheckReplyError`, `CheckRepliesError`, and `CheckTxnRepliesError` to validate replies for single commands, batched commands, and transactional command flows
- Improved error detection in transaction replay paths so failures returned during `MULTI/EXEC` can be surfaced earlier
- Expanded tests around the transaction batcher and output path to reduce the risk of missed reply-side failures

### 2.2 Better Redis Role Detection

- Refactored the `GetRedisRoleOnline` path into smaller parsing steps for better maintainability
- Improved role detection behavior for both standalone and cluster deployments
- Added regression coverage for standalone and cluster role probing, including bisync-related validation

### 2.3 Stronger Test and Release Coverage

- Added bulk dataset generation and validation for non-bisync scenarios to increase workload coverage
- Enhanced the rich workload test to validate multiple key sets
- Added integration tests for Redis reply error checking
- Improved Redis cluster leader-election test stability and isolation
- Added `tests/bisync/run_controlplane_etcd.sh` to cover bisync validation with an etcd control plane

## 3. Compatibility and Upgrade Notes

- `v1.1.1` does not introduce configuration-breaking changes
- Existing `v1.1.0` configurations can be reused directly
- This release mainly strengthens coverage around the following risk areas:
  - Redis commands that appear to execute but return an error reply
  - standalone / cluster role probing mis-detection
  - basic bisync regression coverage under an etcd control plane

## 4. Recommended Upgrade Scenarios

If you are currently using `v1.1.0`, upgrading to `v1.1.1` is recommended when any of the following applies:

- You want earlier detection of Redis reply errors in transactional or batched replay flows
- You run mixed standalone and cluster environments and care about role-detection stability
- You are preparing bisync release validation, especially with an etcd control plane

## 5. Release Guidance

Before production rollout, it is recommended to validate at least:

- baseline sync regression on the target production Redis version
- replay scenarios involving transactions, batched commands, and error replies
- bisync control-plane regression, especially in etcd mode

## 6. Related Commits

- `89cbd9c` `feat(redis): add reply error checking and bulk dataset testing (#105)`
- `0f7db05` `refactor(redis): extract role parsing logic and improve role detection (#107)`
