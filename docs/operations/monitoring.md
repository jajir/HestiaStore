# Monitoring

This document describes runtime monitoring for HestiaStore indexes, with
special focus on WAL-enabled deployments.

For rollout and rollback procedures, see
[WAL Canary Runbook](wal-canary-runbook.md).

## Metrics Source

Use `SegmentIndex.metricsSnapshot()` as the canonical in-process source.
Export these values into your monitoring stack (Micrometer/Prometheus, etc.)
at a fixed scrape interval.

## Core Index Signals

- Throughput:
  - `getOperationCount`, `putOperationCount`, `deleteOperationCount`
- Cache behavior:
  - `registryCacheHitCount`, `registryCacheMissCount`, `registryCacheEvictionCount`
- Segment write path:
  - `totalBufferedWriteKeys`
  - `totalDeltaCacheFiles`
  - `putBusyRetryCount`
  - `putBusyTimeoutCount`
- Latency:
  - `readLatencyP50/P95/P99Micros`
  - `writeLatencyP50/P95/P99Micros`
- State:
  - `state` (`OPENING`, `READY`, `CLOSING`, `ERROR`, `CLOSED`)

## Split And Maintenance Signals

In the current direct-to-segment runtime, these are the primary topology and
maintenance indicators:

- `getSplitScheduleCount()`
- `getSplitInFlightCount()`
- `getSplitBlockedPartitionCount()`
- `getSplitTaskStartDelayP95Micros()`
- `getSplitTaskRunLatencyP95Micros()`
- `getFlushAcceptedToReadyP95Micros()`
- `getCompactAcceptedToReadyP95Micros()`
- `getStableSegmentMaintenanceQueueSize()`
- `getStableSegmentMaintenanceActiveThreadCount()`

Compatibility note:

- partition-named fields remain in `metricsSnapshot()` for backward
  compatibility with older dashboards and management clients
- in the direct-to-segment runtime those partition-overlay counters should
  normally remain `0`
- new dashboards and alerts should prefer the explicit split, maintenance, WAL,
  and segment-write-path fields above

## WAL Signals

Use these fields whenever `isWalEnabled()` is `true`:

- Append throughput:
  - `getWalAppendCount()`
  - `getWalAppendBytes()`
- Sync health:
  - `getWalSyncCount()`
  - `getWalSyncFailureCount()`
  - `getWalSyncAvgNanos()`
  - `getWalSyncMaxNanos()`
  - `getWalSyncAvgBatchBytes()`
  - `getWalSyncBatchBytesMax()`
- Recovery/corruption:
  - `getWalCorruptionCount()`
  - `getWalTruncationCount()`
- Retention/checkpoint:
  - `getWalRetainedBytes()`
  - `getWalSegmentCount()`
  - `getWalCheckpointLsn()`
  - `getWalAppliedLsn()`
  - `getWalCheckpointLagLsn()`
- Backlog:
  - `getWalPendingSyncBytes()`

## Suggested Alerts

Start with these baseline alerts and tune per workload:

- `wal sync failures`:
  - condition: `getWalSyncFailureCount()` increases
  - severity: critical
- `wal corruption detected`:
  - condition: `getWalCorruptionCount()` increases
  - severity: critical
- `unexpected wal truncation`:
  - condition: `getWalTruncationCount()` increases outside controlled recovery
  - severity: high
- `wal retention pressure`:
  - condition: `getWalRetainedBytes()` exceeds 80% of configured
    `maxBytesBeforeForcedCheckpoint` for 10 minutes
  - severity: warning
- `wal checkpoint lag growth`:
  - condition: `getWalCheckpointLagLsn()` grows continuously for 10+ minutes
  - severity: warning
- `wal pending sync growth`:
  - condition: `getWalPendingSyncBytes()` grows without recovery for 10+ minutes
  - severity: warning
- `write busy retries growing`:
  - condition: `getPutBusyRetryCount()` grows continuously together with write
    latency
  - severity: warning
- `split queue delay spike`:
  - condition: `getSplitTaskStartDelayP95Micros()` remains elevated above
    workload baseline for 10+ minutes
  - severity: warning
- `stable maintenance backlog`:
  - condition: `getStableSegmentMaintenanceQueueSize()` stays elevated and
    `getTotalBufferedWriteKeys()` keeps growing
  - severity: warning
- `index stuck closing`:
  - condition: `state == CLOSING` for longer than the expected shutdown window
  - severity: warning

## Structured Logs

Parse and index these WAL events:

- Recovery and repair:
  - `event=wal_recovery_start`
  - `event=wal_recovery_invalid_tail`
  - `event=wal_recovery_tail_repair`
  - `event=wal_recovery_drop_newer_segments`
  - `event=wal_recovery_checkpoint_clamp`
  - `event=wal_recovery_complete`
- Checkpoint and retention:
  - `event=wal_checkpoint_cleanup`
  - `event=wal_retention_pressure_start`
  - `event=wal_retention_pressure_cleared`
- Sync failures:
  - `event=wal_sync_failure`
  - `event=wal_sync_failure_transition`

## Dashboard Minimum

At minimum, create one dashboard per WAL-enabled index with:

1. Write latency (`P50/P95/P99`) and throughput.
2. `WalSyncAvgNanos`, `WalSyncMaxNanos`, `WalSyncCount`.
3. `WalRetainedBytes`, `WalSegmentCount`, `WalCheckpointLagLsn`.
4. `WalPendingSyncBytes`.
5. Counters for `WalSyncFailureCount`, `WalCorruptionCount`,
   `WalTruncationCount`.
6. `TotalBufferedWriteKeys`, `TotalDeltaCacheFiles`,
   `SplitInFlightCount`, `StableSegmentMaintenanceQueueSize`.
7. Index state timeline (`OPENING` / `READY` / `CLOSING` / `ERROR` / `CLOSED`).
