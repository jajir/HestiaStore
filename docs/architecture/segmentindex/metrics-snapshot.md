# Metrics Snapshot

`SegmentIndex.metricsSnapshot()` exposes a stable, read-only metrics contract
for index-level telemetry.

## Current fields

- Operation counters:
  - `getOperationCount`
  - `putOperationCount`
  - `deleteOperationCount`
- Registry and segment state:
  - `registryCache*`
  - `segment*`
  - `totalSegmentKeys`
  - `totalSegmentCacheKeys`
  - `totalWriteCacheKeys`
- Partitioned ingest overlay:
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
- `partitionBufferedKeyCount` counts only overlay-resident keys; it is a
  subset of `totalWriteCacheKeys`.
- Legacy `split*` and `maintenance*` fields remain in the snapshot for
  compatibility, but partition-specific fields are the authoritative view for
  the new ingest runtime.

## Compatibility policy

- Existing fields and meaning are stable and cannot change silently.
- New fields may be added in future versions.
- Consumers should ignore unknown fields for forward compatibility.
