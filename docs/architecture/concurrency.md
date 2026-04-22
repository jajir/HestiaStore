# Concurrency Model

## Overview

`SegmentIndex` is thread-safe and does not use a global read/write lock. Sync
operations run on the caller thread; concurrency is controlled by per-segment
state machines plus a few small shared-structure locks around routing,
registry lifecycle, and split admission.

Segment-level concurrency does not require external locks. `SegmentImpl`
enforces its own admission gate and iterator/version rules; see
[Segment Concurrency](segment/segment-concurrency.md).

## Concurrency Invariants

These invariants must hold in the current direct-to-segment model:

- **Mapping integrity:** the key-to-segment map always points to an existing
  stable segment, and map updates are atomic.
- **Split atomicity:** a route split either publishes both child segments plus
  the updated route map, or it publishes nothing.
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

- `KeyToSegmentMap` for route lookup and atomic split publish
- `SegmentRegistry` and `SegmentDataCache` for segment lifecycle and cached
  data
- `BackgroundSplitCoordinator` and `RouteSplitCoordinator` for split
  scheduling, materialization, and publish admission
- `IndexState` and `Stats`
- any `TypeDescriptor` implementation with mutable internal state

## Ordering

- With multiple threads, operation order is not guaranteed.
- If strict ordering is required, apply external synchronization at the caller.
- Across different routed segments, operations can complete in any order.

## Threads

- Sync operations run on caller threads.
- Segment maintenance runs on the segment maintenance executor.
- Split planning runs on one dedicated split-planner thread.
- Split materialization runs on the shared split-maintenance executor pool.

## Implications

- Read/write conflicts are handled at the routed segment and split-admission
  level, not by a global lock.
- Read-heavy workloads benefit from parallelism across routed segments.
- Writes to a route affected by split can see transient internal `BUSY` and
  are retried by the index retry policy.
- Callers must not rely on a global ordering across keys.
