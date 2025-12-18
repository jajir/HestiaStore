# Concurrency Model

## Overview

The index uses **per-segment serialization**. Operations that target the same segment are serialized; operations targeting different segments may run in parallel.

## Ordering

- With multiple threads, **operation order is not guaranteed**, even within the same segment; lock fairness is not enforced, so waiting threads may be scheduled in any order.
- If strict ordering is required, configure the index with a single CPU thread (`withNumberOfCpuThreads(1)`), which effectively serializes all operations.
- Across segments, operations can complete in any order when more than one thread is configured; with a single thread, all operations are serialized globally.

## Threads

- A fixed-size executor runs index operations. The CPU thread count is configurable (default 1).
- `withNumberOfIoThreads(...)` exists as a separate knob for future work (splitting blocking disk I/O from CPU-bound work).

## Implications

- Write/write and read/write conflicts on the same segment are serialized.
- Read-heavy workloads spanning many segments can benefit from parallelism.
- Callers should not rely on global ordering across keys; only per-segment ordering is guaranteed.
