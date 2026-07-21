# Concurrency Model

## Overview

`SegmentIndex` is thread-safe and does not use a global read/write lock. Sync
operations run on the caller thread; concurrency is controlled by per-segment
state machines plus small shared-structure locks around persisted routing,
runtime route topology, and registry lifecycle.

Segment-level concurrency does not require external locks. `SegmentImpl`
enforces its own admission gate and iterator/version rules; see
[Segment Concurrency](segment/segment-concurrency.md).

## Concurrency Invariants

These invariants must hold in the current direct-to-segment model:

- **Mapping integrity:** the key-to-segment map always points to an existing
  stable segment, and map updates are atomic.
- **Topology handoff:** foreground operations hold a `RouteTopology` route
  lease before touching a segment. Callers acquire this through
  `MappedSegmentLeaseService`, which also loads the mapped segment from
  `SegmentRegistry`. Split materialization starts only after
  `MappedSegmentLeaseService.tryAcquireForSplit(...)` moves the parent route to
  `DRAINING` and all in-flight route leases drain.
- **Split atomicity:** before route-map publish, a split either deletes
  prepared children and keeps the parent route active, or it publishes the
  children into the route map. After route-map publish, recovery cleanup owns
  any orphaned segment directories left by persistence or parent-delete
  failures.
- **Segment-local freshness:** a successful write is immediately visible to a
  later read through the target segment write cache.
- **Lifecycle linearity:** once close starts, no new API operations are
  accepted; in-flight operations either finish or fail deterministically.
- **No use-after-close:** evicted or closed segment resources are not reused by
  concurrent readers.
- **Stats consistency:** counters reflect completed work for the lifetime of
  the index instance.

## Shared State That Must Be Protected

These structures are shared across threads and require synchronization:

- `SegmentRouteMap` for persisted route lookup and atomic split publish
- `RouteTopology` for runtime route states, in-flight route leases, and split
  drain coordination
- `MappedSegmentLeaseService` for the boundary that combines route snapshots,
  topology leases/drains, and registry-loaded `BlockingSegment` handles
- `SegmentRegistry` and `SegmentDataCache` for segment lifecycle and cached
  data
- `SplitPolicyScheduler`, `SplitTaskCoordinator`,
  `RouteSplitPlanner`, and `RouteSplitPublisher` for split
  scheduling, split-lease lifecycle, materialization, and route-map publish
- `IndexState` and `Stats`
- any `TypeDescriptor` implementation with mutable internal state

## Ordering

- With multiple threads, operation order is not guaranteed.
- If strict ordering is required, apply external synchronization at the caller.
- Across different routed segments, operations can complete in any order.

## Threads

- Sync operations run on caller threads.
- Segment maintenance runs on the shared `hestia-segment-maintenance-*`
  executor.
- Split planning runs on the index-owned
  `hestia-<indexName>-split-policy-*` scheduler.
- Split materialization runs on the shared `hestia-split-maintenance-*`
  executor.
- WAL append ordering and delayed group sync both run on the single
  `hestia-<indexName>-wal-append-*` worker. `ExecutorRegistry` supplies its
  index-scoped daemon thread factory; `WalRuntime` drains and joins it.

## Implications

- Read/write conflicts are handled at the routed segment and
  `MappedSegmentLeaseService` route-lease level, not by a global lock.
- Read-heavy workloads benefit from parallelism across routed segments.
- Writes to a route affected by split can see transient internal `BUSY` and
  are retried by the index retry policy.
- Callers must not rely on a global ordering across keys.
