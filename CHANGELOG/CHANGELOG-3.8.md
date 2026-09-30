
<hr>

## v3.8.0

### Bug Fixes
- Add a Ready batch-acknowledgment fence to `Node` (sync storage writes): persisted entries of a Ready batch are not republished before the batch is acknowledged with `Advance`; an accepted but unacknowledged batch survives consumer cancellation and `Stop()` (it stays recoverable from stable storage); and goroutines blocked on `Ready()` now return when `Stop()` closes the channel.

### Features
-

### Others
- [Remove unnecessary log shrinking](https://github.com/etcd-io/raft/pull/462)
