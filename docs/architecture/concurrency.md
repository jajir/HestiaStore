# Concurrency Model

## Overview

The current implementation enforces a **global read/write lock** at the index
level. Read operations can overlap, while any write operation
(`put/delete/flush/compact/close`) is exclusive and blocks other reads and
writes. Segments are also wrapped with per-segment locking, but the index-level
lock is the dominant guard today.

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
