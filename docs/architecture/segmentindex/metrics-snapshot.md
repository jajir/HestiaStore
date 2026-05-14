# Metrics Snapshot

`SegmentIndex.runtimeMonitoring().snapshot().getMetrics()` exposes a stable,
read-only metrics contract for index-level telemetry.

## Ownership Convention

- Domain packages own runtime statistics snapshots and name them `*Stats`.
  Examples include `IndexOperationStats`, `MaintenanceStats`, `SplitStats`,
  `WalStats`, `ChunkStoreCacheStats`, `SegmentRegistryCacheStats`, and
  `SegmentStats`.
- Mutable statistics writers live next to the domain runtime they observe and
  are named `*StatsRecorder` or `*Telemetry`.
- Snapshot-returning runtime methods use `statsSnapshot()`.
- `org.hestiastore.index.segmentindex.metrics` does not record domain events
  directly. It obtains read-only `*Stats` snapshots from operation,
  maintenance, split, WAL, executor, cache, registry, and segment runtimes, then
  assembles the public `SegmentIndexMetricsSnapshot`.

## Current Fields

- Operation counters:
  - `getOperationCount`
  - `putOperationCount`
  - `deleteOperationCount`
- Canonical write-path view:
  - `getWritePathMetrics()`
  - `getSegmentWriteCacheKeyLimit()`
  - `getSegmentWriteCacheKeyLimitDuringMaintenance()`
  - `getIndexBufferedWriteKeyLimit()`
  - `getTotalBufferedWriteKeys()`
  - `getPutBusyRetryCount()`
  - `getPutBusyTimeoutCount()`
- Registry and segment state:
  - `registryCache*`
  - `segment*`
  - `totalSegmentKeys`
  - `totalSegmentCacheKeys`
  - `totalDeltaCacheFiles`
- Maintenance and split:
  - `splitScheduleCount`
  - `splitInFlightCount`
  - `maintenanceQueue*`
  - `splitQueue*`
  - `splitTask*`
  - `drainTask*`
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
- `getWritePathMetrics()` is the canonical write-path view for the
  direct-to-segment runtime.
- `split*`, `maintenance*`, `segment*`, `totalBufferedWriteKeys`, and `wal*`
  fields are the authoritative operational view for the current runtime.
- `state` is one of `OPENING`, `READY`, `CLOSING`, `CLOSED`, or `ERROR`.
- `CLOSING` means shutdown is in progress and the index is no longer accepting
  new API operations, but final persistence/cleanup work may still be running.

## Compatibility Policy

- Existing fields and meaning are stable within the current public API.
- New fields may be added in future versions.
- Consumers should ignore unknown fields for forward compatibility.
