# Metrics RouteMapSnapshot

`SegmentIndex.runtimeMonitoring().snapshot()` exposes the stable,
read-only runtime monitoring contract for index-level telemetry.

## Ownership Convention

- Runtime packages own their raw monitoring snapshots and name them `*Stats`
  or `*Monitoring` according to the local package language.
  Examples include `OperationStatsSnapshot`, `MaintenanceStatsSnapshot`, `SplitStats`,
  `WalMonitoring`, `ChunkStoreCacheStats`, `SegmentRegistryCacheStats`, and
  `SegmentStats`.
- Mutable statistics writers live next to the domain runtime they observe and
  are named `*StatsRecorder` or `*Telemetry`.
- RouteMapSnapshot-returning runtime methods use `statsSnapshot()`.
- `org.hestiastore.index.segmentindex.monitoring` does not record domain
  events directly. It obtains read-only package-owned snapshots from operation,
  maintenance, split, WAL, executor, cache, registry, and segment runtimes, then
  projects them into the public grouped model under
  `org.hestiastore.index.segmentindex.monitoring.model`.

## Public Groups

- `snapshot.operations()`
- `snapshot.registryCache()`
- `snapshot.chunkStoreCache()`
- `snapshot.segments()`
- `snapshot.writePath()`
- `snapshot.maintenance()`
- `snapshot.split()`
- `snapshot.latency()`
- `snapshot.bloomFilter()`
- `snapshot.wal()`
- `snapshot.state()` for the index lifecycle state

## Semantics

- RouteMapSnapshot is immutable.
- Counts are monotonic for one index instance lifetime.
- Counters are process-local and reset when a new index object is created.
- Field values represent observed operation calls, not necessarily durable
  writes on disk.
- `writePath()` is the canonical write-path view for the direct-to-segment
  runtime.
- `split()`, `maintenance()`, `segments()`, `writePath()`, and `wal()` are the
  authoritative operational view for the current runtime.
- `snapshot.state()` is one of `OPENING`, `READY`, `CLOSING`, `CLOSED`, or
  `ERROR`.
- `CLOSING` means shutdown is in progress and the index is no longer accepting
  new API operations, but final persistence/cleanup work may still be running.
