# Monitoring

This document describes runtime monitoring for HestiaStore indexes, with
special focus on WAL-enabled deployments.

For rollout and rollback procedures, see
[WAL Canary Runbook](wal-canary-runbook.md).

## Metrics Source

Use `SegmentIndex.runtimeMonitoring().snapshot()` as the canonical in-process source.
Export these values into your monitoring stack (Micrometer/Prometheus, etc.)
at a fixed scrape interval.

For in-process integrations, use the grouped runtime monitoring model:

- `snapshot.operations()`
- `snapshot.segments()`
- `snapshot.writePath()`
- `snapshot.maintenance()`
- `snapshot.split()`
- `snapshot.wal()`

## Core Index Signals

- Throughput:
  - `operations().readOperationCount()`, `operations().putOperationCount()`,
    `operations().deleteOperationCount()`
- Cache behavior:
  - `registryCache().hitCount()`, `registryCache().missCount()`,
    `registryCache().evictionCount()`
- Segment write path:
  - `writePath().totalBufferedWriteKeys()`
  - `segments().totalDeltaCacheFiles()`
- Latency:
  - `latency().readP50Micros()`, `latency().readP95Micros()`,
    `latency().readP99Micros()`
  - `latency().writeP50Micros()`, `latency().writeP95Micros()`,
    `latency().writeP99Micros()`
- State:
  - `snapshot().state()` (`OPENING`, `READY`, `CLOSING`, `ERROR`, `CLOSED`)

## Split And Maintenance Signals

In the current direct-to-segment runtime, these are the primary topology and
maintenance indicators:

- `split().scheduleCount()`
- `split().inFlightCount()`
- `split().blockedCount()`
- `split().taskStartDelayP95Micros()`
- `split().taskRunLatencyP95Micros()`
- `maintenance().flushAcceptedToReadyP95Micros()`
- `maintenance().compactAcceptedToReadyP95Micros()`
- `maintenance().stableSegmentExecutor().queueSize()`
- `maintenance().stableSegmentExecutor().activeThreadCount()`

## WAL Signals

Use these fields whenever `wal().enabled()` is `true`:

- Append throughput:
  - `wal().appendCount()`
  - `wal().appendBytes()`
- Sync health:
  - `wal().syncCount()`
  - `wal().syncFailureCount()`
  - `wal().syncAverageNanos()`
  - `wal().syncMaxNanos()`
  - `wal().syncAverageBatchBytes()`
  - `wal().syncBatchBytesMax()`
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
