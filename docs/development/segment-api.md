# Segment API: Flush, Compact, and Split

## Scope

This document explains how the public `Segment` API behaves during maintenance
and how segment-level rules interact with index-level route splitting.

The public `Segment` interface exposes `flush()` and `compact()`. Split is
owned by the segment-index layer through
`BackgroundSplitCoordinator` + `RouteSplitCoordinator`.

For the current state machine and iterator rules, use
[Segment Concurrency](../architecture/segment/segment-concurrency.md).

## Current Model

- `Segment.put(...)` writes into the segment write cache.
- `Segment.get(...)` reads write cache first, then delta cache, then stable
  files.
- `flush()` freezes the current write-cache snapshot and persists it into delta
  cache files.
- `compact()` rewrites the stable segment view from main SST + delta cache and
  clears obsolete delta files.
- `SegmentIndex` routes writes directly to a stable segment; there is no
  index-level ingest overlay anymore.

## Flush

Current behavior:

- under `FREEZE`, the segment swaps out the current write cache
- in `MAINTENANCE_RUNNING`, the frozen snapshot is written to delta cache
  files
- under `FREEZE`, metadata is published and iterators are invalidated
- new writes continue in a fresh write cache while the frozen snapshot is being
  persisted

Practical consequences:

- `flush()` is a no-op when the write cache is empty
- concurrent `flush()` requests return `BUSY`
- fail-fast iterators can terminate early after the publish step

## Compact

Current behavior:

- compaction captures the current stable view plus in-memory deltas
- background I/O builds a new SST, sparse index, and Bloom filter
- publish swaps the new stable files atomically
- obsolete delta files are removed after a successful publish

Practical consequences:

- concurrent `compact()` requests return `BUSY`
- fail-fast iterators can terminate early after publish
- `get()` remains thread-safe; it either sees the old view or the new view

## Split

Current index-level split behavior:

- `BackgroundSplitCoordinator` decides when a routed segment should be split
- `RouteSplitCoordinator` computes the split boundary from a parent segment
  snapshot
- child stable segments are materialized before route-map publish
- publish is a short exclusive update of `KeyToSegmentMap`
- writes to the affected route may see transient internal `BUSY` and are
  retried by `IndexRetryPolicy`

Important boundary:

- `Segment` itself does not own route-map changes
- `Segment` only provides the stable snapshot and local read-after-write
  guarantee used by the split coordinator

## Backpressure And Overload

There is no index-level ingest overlay to absorb writes anymore. Backpressure
comes from:

- segment-local write cache and delta-cache maintenance limits
- transient `BUSY` while a segment is in `FREEZE` or maintenance
- transient `BUSY` while a route is scheduled for split
- WAL retention pressure when WAL is enabled

Legacy-named compatibility settings still exist in `IndexConfiguration`, but
they now tune routed segment write-cache, maintenance backlog, and split
thresholds rather than a separate partition runtime.

## Parallel Calls

Same segment:

- `flush()`, `compact()`, and `FULL_ISOLATION` iteration are exclusive
- writes can continue during `MAINTENANCE_RUNNING` when the segment state
  allows it

Different segments:

- maintenance can run in parallel
- split work on one route does not stop unrelated routes

## Reads And Iterators

- multiple `get()` calls can run concurrently
- `FAIL_FAST` iteration is optimistic and can stop early after a publish
- `FULL_ISOLATION` iteration holds exclusive access for its lifetime and blocks
  writes, flush, compact, and split materialization on that segment

## Corner Cases

- always close `FULL_ISOLATION` iterators; otherwise writers and split
  materialization can stall
- calls on a closed segment return `SegmentResultStatus.CLOSED`
- version overflow still fails fast in `VersionController`
- stale references to a retired parent segment must not be reused after split
- Run maintenance on a background executor and prioritize older snapshots.

### Parallel calls

- One maintenance task per segment at a time; other requests coalesce or wait.
- Flush requests during split should attach to replay or run after split.
- Maintenance can still run in parallel across different segments.

### Reads, get(), and iterators

- `get()` should read from current caches and files without blocking on long
  maintenance tasks.
- Fail-fast iterators continue to invalidate on version changes.
- Prefer snapshot-based iterators for long scans instead of holding write
  holds for `FULL_ISOLATION`.

### Corner cases

- Version mismatch at swap time should trigger retry or fallback.
- Large post-freeze write backlog should trigger backpressure or replay caps.
- Segment replacement must be atomic in the registry to avoid stale reads.
- Iterators opened before a swap should either complete on their snapshot or
  fail fast.

### Coordination summary

- Per-segment `FREEZE` for short cache/file swaps.
- Per-segment maintenance state to serialize flush/compact/split work.
- Optimistic version counter for iterator invalidation.
- Registry lock for file replacement if added in the future.
