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
- Latency:
  - `readLatencyP50/P95/P99Micros`
  - `writeLatencyP50/P95/P99Micros`
- State:
  - `state` (`OPEN`, `ERROR`, `CLOSED`, ...)

## Partition Overlay Signals

For the range-partitioned ingest runtime, treat these as the primary
backpressure and drain indicators:

- Buffered overlay pressure:
  - `getPartitionBufferedKeyCount()`
  - `getImmutableRunCount()`
  - `getDrainingPartitionCount()`
- Capacity and routing shape:
  - `getPartitionCount()`
  - `getActivePartitionCount()`
  - `getMaxNumberOfKeysInActivePartition()`
  - `getMaxNumberOfKeysInPartitionBuffer()`
  - `getMaxNumberOfKeysInIndexBuffer()`
  - `getMaxNumberOfImmutableRunsPerPartition()`
- Throttling:
  - `getLocalThrottleCount()`
  - `getGlobalThrottleCount()`
- Drain activity:
  - `getDrainScheduleCount()`
  - `getDrainInFlightCount()`
  - `getDrainLatencyP95Micros()`

Compatibility note:

- `splitScheduleCount`, `splitInFlightCount`, `maintenanceQueueSize`, and
  related legacy queue fields are still emitted for older clients.
- New dashboards and alerts should prefer the explicit partition fields above.

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
- `partition overlay backlog growth`:
  - condition: `getPartitionBufferedKeyCount()` and `getImmutableRunCount()`
    grow continuously without returning to baseline
  - severity: warning
- `partition drain latency spike`:
  - condition: `getDrainLatencyP95Micros()` remains elevated above workload
    baseline for 10+ minutes
  - severity: warning
- `partition throttling`:
  - condition: `getLocalThrottleCount()` or `getGlobalThrottleCount()`
    increases steadily
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
6. `PartitionBufferedKeyCount`, `ImmutableRunCount`,
   `DrainingPartitionCount`, `DrainInFlightCount`.
7. Index state timeline (`OPEN` vs `ERROR`).
