# Write Path

This page describes how a write travels through HestiaStore from the API call
to on-disk structures, highlighting routing, segment-local buffering,
compaction, and atomicity. It maps directly to the code so you can cross-check
behavior and tune configuration.

Segment-internal structures are centralized in
[Segment Architecture](../segment/index.md). This page focuses on SegmentIndex
orchestration and operation flow.

## High‑Level Flow

1. API call: `SegmentIndex.put(key, value)` or `SegmentIndex.delete(key)`
1. Append the logical operation to WAL when WAL is enabled
1. Resolve the routed segment through `KeyToSegmentMap`
1. Write directly into the target segment
1. Segment-local maintenance later flushes or compacts that segment
1. Autonomous split policy may remap hot routes to child segments

Writes become durable when flushed to segment files. Closing the index performs
a flush.

## Entry Points

- `SegmentIndex.put(K,V)` and `SegmentIndex.delete(K)` validate input, update
  counters, and delegate to the internal implementation.
- Internal implementation: `IndexInternalConcurrent` (caller-thread execution,
  thread-safe without global serialization).
- Async operations are executed by the dedicated `IndexAsyncExecutor` inside
  the internal implementation; `IndexAsyncAdapter` is a thin async-facing
  wrapper and `IndexContextLoggingAdapter` adds MDC correlation.

Key classes:
`segmentindex/SegmentIndex.java`,
`segmentindex/IndexInternalConcurrent.java`,
`segmentindex/core/IndexAsyncExecutor.java`,
`segmentindex/IndexAsyncAdapter.java`,
`segmentindex/IndexContextLoggingAdapter.java`.

## Optional Logging Context

If `IndexConfiguration.isContextLoggingEnabled()` is true, index operations
populate the `index.name` MDC key so downstream logs can include the index
identifier. This is purely for log correlation and does not write any
additional files or provide durability.

## Routed Direct Writes

Every `put` / `delete` now goes straight to the routed stable segment:

- `IndexOperationCoordinator` appends to WAL first when enabled
- `DirectSegmentWriteCoordinator` resolves the current route
- `StableSegmentGateway` loads the segment and calls `Segment.put(...)`
- read-after-write is then guaranteed by the target segment's write cache

Key classes:
`segmentindex/core/IndexOperationCoordinator`,
`segmentindex/core/DirectSegmentWriteCoordinator`,
`segmentindex/core/StableSegmentGateway`,
`segment/SegmentImpl`.

## Flush and Segment Maintenance

SegmentIndex no longer has an index-level drain layer. Maintenance now works
directly on mapped segments:

1. Wait for already-scheduled split work to settle.
1. Flush or compact mapped stable segments.
1. Re-check the route map in case a split published during maintenance.
1. Flush `index.map`.
1. Checkpoint WAL on `flushAndWait()` / `compactAndWait()`.

Key classes:
`segmentindex/core/IndexMaintenanceCoordinator`,
`segmentindex/core/StableSegmentCoordinator`,
`segmentindex/mapping/KeyToSegmentMap`.

## Segment Delta Cache Files (Transactional)

Writes land in a segment's delta cache as sorted key/value files. Each delta
file is written transactionally:

- data is written to `vNN-delta-NNNN.cache.tmp` and atomically renamed on
  commit
- segment properties track counts and delta file numbering
- if the segment data is currently cached in memory, the delta cache is also
  updated in-memory to keep reads fresh

Key classes:
`segment/SegmentDeltaCacheWriter`,
`segment/SegmentPropertiesManager`,
`sorteddatafile/SortedDataFileWriterTx`.

## On‑Disk Merge (Compaction)

Compaction merges the main SST with all delta cache files into a new
consistent state and rebuilds auxiliary structures:

- main SST (chunked file) written via `ChunkEntryFileWriter` and
  `ChunkStoreWriterTx`
- sparse index ("scarce index") updated every Nth key to accelerate seeks
- Bloom filter rebuilt from keys to accelerate negative lookups
- delta cache is cleared on successful commit

Triggers:

- opportunistic: after delta writes, if policy advises compaction
- forced: explicitly via `compact()` or before certain operations like
  splitting

Atomicity:

- all writers use temp files (`.tmp`) and `rename` to commit
- Bloom filter writes inside a dedicated transaction (`BloomFilterWriterTx`)

Key classes:
`segment/SegmentCompacter`,
`segment/SegmentFullWriterTx`,
`segment/SegmentFullWriter`,
`bloomfilter/BloomFilterWriterTx`,
`scarceindex/*`.

## Segment Splitting

When a routed segment grows beyond `maxNumberOfKeysInPartitionBeforeSplit`,
the split coordinator computes a route-first split plan, materializes child
stable segments from the parent stable snapshot, and atomically updates the
key-to-segment mapping.

Key classes:
`segmentindex/core/BackgroundSplitCoordinator`,
`segmentindex/split/RouteSplitCoordinator`,
`segmentindex/split/RouteSplitPlan`,
`segmentindex/mapping/KeyToSegmentMap`.

## Delete Semantics (Tombstones)

Deletes write a tombstone value:

- routed to the target segment like any other update
- during compaction, tombstones suppress older values and may be dropped if
  safe
- reads treat tombstones as absent

Key classes:
`segmentindex/core/IndexOperationCoordinator#delete`,
`datatype/TypeDescriptor#getTombstone`,
`segment/SegmentSearcher`.

## Durability and Atomicity

- transactional writers use a temp file + atomic rename to ensure either the
  old state or the new state is visible after a crash
- `close()` and explicit `flushAndWait()` drive persistence of buffered writes
- context logging is not a durability mechanism

## Configuration Knobs Affecting Writes

- `maxNumberOfKeysInActivePartition` — legacy-named compatibility limit now
  used as the routed segment write-cache threshold
- `maxNumberOfKeysInPartitionBuffer` — legacy-named compatibility limit used
  as the per-segment maintenance/write-buffer ceiling
- `maxNumberOfKeysInIndexBuffer` — index-level compatibility budget exposed in
  metrics and runtime tuning
- `maxNumberOfKeysInSegmentCache` — bounds total in-segment cache size before
  compaction and split decisions
- `maxNumberOfKeysInSegmentChunk` — controls sparse index sampling cadence
- `maxNumberOfKeysInPartitionBeforeSplit` — split threshold per routed range
- `bloomFilter*` — Bloom filter size/hash tuning
- `diskIoBufferSize` — I/O buffer sizing for on-disk writers
- `encoding/decodingChunkFilters` — write/read pipelines (e.g. Snappy, CRC32,
  magic number)

See: `segmentindex/IndexConfiguration` and
`segmentindex/IndexConfigurationBuilder`.

## Integrity Filters on the Write Path

The chunk writer applies a filter pipeline when persisting chunk payloads:

- magic number writing
- CRC32 computation
- optional Snappy compression

These produce a self-describing chunk header and robust payload handling.

Key classes:
`chunkstore/ChunkProcessor`,
`chunkstore/ChunkFilterMagicNumberWriting`,
`chunkstore/ChunkFilterCrc32Writing`,
`chunkstore/ChunkFilterSnappyCompress`.

## Sequence (Put)

1. `SegmentIndex.put(k,v)` → validate inputs; forbid direct tombstone values
1. Append to WAL (when enabled)
1. Resolve write route via key→segment map
1. Write latest `(k,v)` into the routed segment write cache
1. If the route is split-blocked or the target segment is transiently busy:
   retry through `IndexRetryPolicy`

## Where to Look in the Code

- SegmentIndex entry points and routing:
  `src/main/java/org/hestiastore/index/segmentindex/core/SegmentIndexImpl.java`
- Direct routed write path:
  `src/main/java/org/hestiastore/index/segmentindex/core/DirectSegmentWriteCoordinator.java`
- Segment write/merge path:
  `src/main/java/org/hestiastore/index/segment/*`
- Chunk store and filters:
  `src/main/java/org/hestiastore/index/chunkstore/*`
- Delta and sorted file writers:
  `src/main/java/org/hestiastore/index/sorteddatafile/*`

For the read path and on-disk layout, see the related pages:

- [Read Path](read-path.md)
- [On‑Disk Layout & File Names](../segment/on-disk-layout.md)
- [Filters & Integrity](../filters.md)

## Related Glossary

- [Segment](../glossary.md#segment)
- [Delta Cache](../glossary.md#delta-cache)
- [Flush](../glossary.md#flush)
- [Compaction](../glossary.md#compaction)
- [Split](../glossary.md#split)
- [Write Transaction](../glossary.md#write-transaction)
- [Filters](../glossary.md#filters-chunk-filters)
- [Tombstone](../glossary.md#tombstone)
