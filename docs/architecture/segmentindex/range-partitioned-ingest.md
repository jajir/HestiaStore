# Range-Partitioned Ingest Architecture

This document describes a proposed write-path redesign for `SegmentIndex`
that keeps `put()` immediately visible to `get()` while removing long-running
split and compaction work from the hot write path.

The proposal does not remove the existing `segment` package. Instead:

- `segment` remains the home of stable immutable segment storage.
- a new package above it owns write buffering, routing, and draining of hot
  ranges.

Proposed package: `org.hestiastore.index.segmentindex.partition`

## Goals

- Keep `put()` visible to `get()` immediately after the call returns.
- Prevent `put()` from waiting on split or compaction of the same hot range.
- Bound the number of pending mutable/immutable write layers.
- Localize overload to a hot range instead of turning the whole index into a
  retry storm.
- Keep stable immutable segments as the unit of durable read storage.

## High-Level Model

`SegmentIndex` no longer writes directly into live `Segment` instances.
Instead, each key range is assigned to a `RangePartition` that owns a short
write pipeline:

- one active mutable layer
- a small immutable queue, typically max `2`
- references to stable immutable segments for the same range

`get(key)` resolves the owning partition and reads only inside that partition:

1. active mutable layer
2. immutable runs from newest to oldest
3. stable immutable segments

This keeps immediate read-after-write visibility without requiring background
flush work to finish first.

Diagram PNG:
![Range-partitioned ingest overview](images/range-partitioned-ingest-overview.png)

PlantUML source:
[`docs/architecture/segmentindex/images/range-partitioned-ingest-overview.plantuml`](images/range-partitioned-ingest-overview.plantuml)

## Partition Components

Each `RangePartition` is responsible for a single key interval and owns:

- `PartitionMutableLayer`: latest in-memory writes for the range
- `PartitionImmutableRun`: sealed write batch waiting for flush
- `stableSegments`: references to immutable on-disk segments
- `PartitionBudget`: local memory and backlog budget
- `PartitionState`: `ACTIVE`, `DRAINING`, or `SPLITTING`

Top-level routing is split into two maps:

- `WriteRouteMap`: tells `put()` where new writes must go
- `ReadRouteMap`: tells `get()` which sources are still valid during
  transition

The two maps are intentionally separate so that writes can move to new child
partitions before the old stable data is fully rewritten.

## Write Path

`put(key, value)` performs only short local work:

1. resolve partition in `WriteRouteMap`
2. append to WAL
3. insert into partition active mutable layer
4. rotate the active layer when it reaches its local budget
5. if the partition immutable queue is full, apply local backpressure

Rotation is local to one partition. A hot range does not force unrelated
ranges to rotate or stall.

Diagram PNG:
![Range-partitioned ingest write sequence](images/range-partitioned-ingest-sequence.png)

PlantUML source:
[`docs/architecture/segmentindex/images/range-partitioned-ingest-sequence.plantuml`](images/range-partitioned-ingest-sequence.plantuml)

## Read Path

`get(key)` must remain cheap even when background flush is behind.

The design depends on two limits:

- per-partition immutable queue is small, typically max `2`
- the number of stable sources per read route is bounded

Lookup order inside one partition:

1. active mutable layer
2. immutable run queue, newest first
3. stable segments

Each source should expose cheap negative-check metadata, for example:

- `minKey/maxKey`
- Bloom filter
- compact in-memory index for the immutable run

This prevents `get()` from degenerating into a scan across all pending writes
in the whole index.

## Split Without Blocking `put()`

The current design couples split with a live segment and holds exclusivity for
too long. The proposed design splits the range-routing decision from the
background rewrite work.

Split flow:

1. a partition `P=[A..Z]` becomes too large or too hot
2. create new child partitions `P1=[A..M]` and `P2=[N..Z]`
3. atomically switch `WriteRouteMap` so all new writes go directly to `P1/P2`
4. old partition `P` moves to `DRAINING` and stops accepting new writes
5. background work rewrites old stable data and old immutable runs into new
   stable segments for `P1/P2`
6. during the transition, `ReadRouteMap` exposes:
   - overlays from `P1/P2`
   - old stable sources from `P`
7. after publish, `ReadRouteMap` is switched to the final `P1/P2` stable
   sources and `P` is removed

Diagram PNG:
![Range-partitioned split drain flow](images/range-partitioned-split-drain.png)

PlantUML source:
[`docs/architecture/segmentindex/images/range-partitioned-split-drain.plantuml`](images/range-partitioned-split-drain.plantuml)

## Backpressure Model

This architecture assumes bounded buffering. Immutable runs must never grow
without limit.

Recommended first version:

- `1` active mutable layer per partition
- max `2` immutable runs per partition
- per-partition memory budget
- global memory budget across all partitions

Backpressure policy:

- if one hot partition fills its immutable queue, throttle only that partition
- if the global memory budget is exhausted, apply global ingest throttling

This replaces the current near-deadlock behavior with explicit overload
handling.

## Package Boundary Proposal

Existing package kept:

- `org.hestiastore.index.segment`
  - stable immutable segment files
  - Bloom filter, sparse index, on-disk readers
  - stable compaction output

New package proposed:

- `org.hestiastore.index.segmentindex.partition`
  - `RangePartition`
  - `PartitionMutableLayer`
  - `PartitionImmutableRun`
  - `WriteRouteMap`
  - `ReadRouteMap`
  - `PartitionFlushScheduler`
  - `PartitionSplitCoordinator`
  - `PartitionBudget`

Expected package responsibility change:

- `segment` stops being the primary live write admission object
- new partition package becomes the hot write/read overlay layer
- stable segment objects are loaded for read storage, not for per-write
  mutation

## Impact on Current Architecture

This proposal is intentionally incremental at the package level:

- keep `segment`
- add a new top-level partition layer above it
- route `put()` and `get()` through partitions first
- keep stable segment publishing as the durable immutable storage boundary

The biggest behavioral change is that split no longer means "freeze the live
segment and make writers wait". Instead, split becomes "move new writes to new
partitions immediately and drain old data in the background".
