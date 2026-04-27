# Metrics Snapshot

`SegmentIndex.metricsSnapshot()` exposes a stable, read-only metrics contract
for index-level telemetry.

## Current fields

- Operation counters:
  - `getOperationCount`
  - `putOperationCount`
  - `deleteOperationCount`
- Canonical write-path view:
  - `getWritePathMetrics()`
  - `getSegmentWriteCacheKeyLimit()`
  - `getSegmentWriteCacheKeyLimitDuringMaintenance()`
  - `getIndexBufferedWriteKeyLimit()`
- Registry and segment state:
  - `registryCache*`
  - `segment*`
  - `totalSegmentKeys`
  - `totalSegmentCacheKeys`
  - `totalBufferedWriteKeys`
- Compatibility fields retained from the removed partition runtime:
  - `getLegacyPartitionCompatibilityMetrics()`
  - `maxNumberOfKeysInActivePartition`
  - `maxNumberOfImmutableRunsPerPartition`
  - `maxNumberOfKeysInPartitionBuffer`
  - `maxNumberOfKeysInIndexBuffer`
  - `partitionCount`
  - `activePartitionCount`
  - `drainingPartitionCount`
  - `immutableRunCount`
  - `partitionBufferedKeyCount`
  - `localThrottleCount`
  - `globalThrottleCount`
  - `drainScheduleCount`
  - `drainInFlightCount`
  - `drainLatencyP95Micros`
- WAL and latency:
  - `wal*`
  - `readLatencyP50/P95/P99Micros`
  - `writeLatencyP50/P95/P99Micros`
- Lifecycle:
  - `state`

## Semantics

- Snapshot is immutable.
- Counts are monotonic for one index instance lifetime.
- Counters are process-local and reset when a new index object is created.
- Field values represent observed operation calls, not necessarily durable
  writes on disk.
- `getWritePathMetrics()` is the canonical write-path view for the current
  direct-to-segment runtime
- partition-named fields are compatibility values; in the direct-to-segment
  runtime they typically remain `0` or act as legacy-named tuning limits
- `split*`, `maintenance*`, `segment*`, `totalBufferedWriteKeys`, and `wal*`
  fields are the authoritative operational view for the current runtime
- `state` is one of `OPENING`, `READY`, `CLOSING`, `CLOSED`, or `ERROR`.
- `CLOSING` means shutdown is in progress and the index is no longer accepting
  new API operations, but final persistence/cleanup work may still be running.

## Compatibility policy

- Existing fields and meaning are stable and cannot change silently.
- New fields may be added in future versions.
- Consumers should ignore unknown fields for forward compatibility.
- New in-process consumers should prefer canonical write-path metrics first and
  only read partition-named fields when integrating with an older compatibility
  contract.
