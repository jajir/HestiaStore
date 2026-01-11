# Concurrency Model

## Overview

SegmentIndex is thread-safe and does not use a global read/write lock. Sync
operations execute on the caller thread; concurrency is controlled by
per-segment state machines and minimal shared-structure locks (mapping,
registry).

Segment-level concurrency does not require external locks. `SegmentImpl`
enforces its own lock-free admission gate (see
`docs/architecture/segment-concurrency.md`).

## Concurrency Invariants (Target Design)

These invariants must hold for the current per-segment concurrency design:

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

These structures are shared across threads and require synchronization:

- `UniqueCache` (index write buffer)
- `KeySegmentCache` (key→segment map)
- `SegmentRegistry` and `SegmentDataCache` (segment lookup + cached data)
- `SegmentSplitCoordinator` (topology updates)
- `IndexState` (open/close state) and `Stats` (counters)
- Any `TypeDescriptor` implementation with mutable internal state

## Ordering

- With multiple threads, **operation order is not guaranteed**.
- If strict ordering is required, apply external synchronization at the caller
  level.
- Across segments, operations can complete in any order.

## Threads

- Sync operations run on caller threads.
- Async operations run on background threads via `IndexAsyncAdapter`.
- Segment maintenance runs on the segment-index maintenance executor.

## Implications

- Write/write and read/write conflicts are handled by per-segment state
  machines and shared-structure locks, not a global lock.
- Read-heavy workloads benefit from parallelism.
- Callers should not rely on global ordering across keys.
