# Concurrency Model

## Overview

The current implementation enforces a **global read/write lock** at the index
level. Read operations can overlap, while any write operation
(`put/delete/flush/compact/close`) is exclusive and blocks other reads and
writes. Segments are also wrapped with per-segment locking, but the index-level
lock is the dominant guard today.

## Concurrency Invariants (Target Design)

These invariants must hold regardless of how locks are implemented during the
per-segment concurrency refactor:

- **Mapping integrity:** the key→segment map must always point to an existing
  segment; updates are atomic and visible to readers.
- **Split atomicity:** a segment split must either be fully applied (new
  segments + updated map) or not applied at all.
- **Cache visibility:** reads observe the latest cached writes (read-after-write
  visibility) and flushes operate on a consistent snapshot.
- **Lifecycle linearity:** once close starts, no new operations are accepted;
  operations in flight either complete before close returns or fail deterministically.
- **No use-after-close:** evicted/closed segment data must not be accessed by
  concurrent readers.
- **Stats consistency:** counters reflect all completed operations (exact or
  eventually consistent by design).

## Shared State That Must Be Protected

These structures are shared across threads and require synchronization when
moving away from the global lock:

- `UniqueCache` (index write buffer)
- `KeySegmentCache` (key→segment map)
- `SegmentRegistry` and `SegmentDataCache` (segment lookup + cached data)
- `SegmentSplitCoordinator` (topology updates)
- `IndexState` (open/close state) and `Stats` (counters)
- Any `TypeDescriptor` implementation with mutable internal state

## Target Locking Plan

Planned locking strategy for per-segment concurrency:

- **Mapping/lifecycle RW lock:** read lock for `get/put/delete` to safely read
  shared structures; write lock for `flush/compact/close` and for segment splits.
- **Per-segment lock:** held only while operating on a specific segment (delta
  writes, compaction, reads).

This allows parallel operations across different segments while keeping shared
state consistent.

## Ordering

- With multiple threads, **operation order is not guaranteed**; the global
  lock favors fairness to reduce write starvation, but it does not impose a
  strict ordering.
- If strict ordering is required, configure the index with a single CPU thread
  (`withNumberOfCpuThreads(1)`), which serializes all operations.
- Across segments, operations can complete in any order when more than one
  thread is configured.

## Threads

- A fixed-size executor runs index operations. The CPU thread count is configurable (default 1).
- `withNumberOfIoThreads(...)` exists as a separate knob for future work (splitting blocking disk I/O from CPU-bound work).

## Implications

- Write/write and read/write conflicts are serialized globally.
- Read-heavy workloads benefit the most from parallelism.
- Callers should not rely on global ordering across keys.
