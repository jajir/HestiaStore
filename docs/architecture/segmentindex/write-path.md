# ✍️ Write Path

This page describes how a write travels through HestiaStore from the API call to on‑disk structures, highlighting buffering, compaction, and atomicity. It maps directly to the code so you can cross‑check behavior and tune configuration.

Segment-internal structures are centralized in
[Segment Architecture](../segment/index.md). This page focuses on SegmentIndex
orchestration and operation flow.

## 🧭 High‑Level Flow

1. API call: `SegmentIndex.put(key, value)` or `SegmentIndex.delete(key)`
1. Routed partition mutable layer accepts the latest value per key
1. Mutable layer rotates to immutable runs when active-partition budget is reached
1. Background drain publishes immutable runs to stable segment storage
1. Autonomous split policy may remap hot ranges to child routes

Writes become durable when flushed to segment files. Closing the index performs a flush.

## 🚪 Entry Points

- `SegmentIndex.put(K,V)` and `SegmentIndex.delete(K)` validate input, update counters, and delegate to the internal implementation.
- Internal implementation: `IndexInternalConcurrent` (caller-thread execution,
  thread-safe without global serialization).
- Async operations are provided by `IndexAsyncAdapter`; logging context by
  `IndexContextLoggingAdapter`.

Key classes: `segmentindex/SegmentIndex.java`,
`segmentindex/IndexInternalConcurrent.java`,
`segmentindex/IndexAsyncAdapter.java`,
`segmentindex/IndexContextLoggingAdapter.java`.

## 🗒️ Optional Logging Context

If `IndexConfiguration.isContextLoggingEnabled()` is true, index operations
populate the `index.name` MDC key so downstream logs can include the index
identifier. This is purely for log correlation and does not write any
additional files or provide durability.

## 🧰 Partition Overlay (Index‑Level)

Every `put`/`delete` first updates the routed partition overlay:

- active mutable layer stores latest values for the partition currently
  accepting writes
- immutable runs keep rotated snapshots queued for drain
- reads consult overlay first, so read-after-write holds before drain publish

Key classes:
`segmentindex/partition/PartitionRuntime`,
`segmentindex/partition/RangePartition`,
`segmentindex/core/SegmentIndexImpl`.

## 🚚 Flush and Routing to Segments

On drain/flush, immutable partition runs are sorted and published to target
stable segments based on the key‑to‑segment map.

Flow:

1) Rotate active mutable layer into an immutable run.
2) Drain immutable runs in the background.
3) Route each entry to target stable segment id via key→segment map.
4) Publish entries into stable segment storage and flush stable segments.
5) Persist routing metadata and checkpoint WAL on `flushAndWait()`.

Key classes: `segmentindex/core/SegmentIndexImpl`, `segmentindex/partition/PartitionRuntime`, `segmentindex/mapping/KeyToSegmentMap`.

## 🗂️ Segment Delta Cache Files (Transactional)

Writes land in a segment’s delta cache as sorted key/value files. Each delta file is written transactionally:

- Data is written to `vNN-delta-NNNN.cache.tmp` and atomically renamed on commit.
- Segment properties track counts and delta file numbering.
- If the segment data is currently cached in memory, the delta cache is also updated in‑memory to keep reads fresh.

Key classes: `segment/SegmentDeltaCacheWriter`, `segment/SegmentPropertiesManager`, `sorteddatafile/SortedDataFileWriterTx`.

## 🧹 On‑Disk Merge (Compaction)

Compaction merges the main SST with all delta cache files into a new consistent state and rebuilds auxiliary structures:

- Main SST (chunked file) written via `ChunkEntryFileWriter` and `ChunkStoreWriterTx`.
- Sparse index ("scarce index") updated every Nth key to accelerate seeks.
- Bloom filter rebuilt from keys to accelerate negative lookups.
- Delta cache is cleared on successful commit.

Triggers:

- Opportunistic: after delta writes, if policy advises compaction.
- Forced: explicitly via `compact()` or before certain operations like splitting.

Atomicity:

- All writers use temp files (`.tmp`) and `rename` to commit.
- Bloom filter writes inside a dedicated transaction (`BloomFilterWriterTx`).

Key classes: `segment/SegmentCompacter`, `segment/SegmentFullWriterTx`, `segment/SegmentFullWriter`, `bloomfilter/BloomFilterWriterTx`, `scarceindex/*`.

## ✂️ Segment Splitting

When a routed segment grows beyond `maxNumberOfKeysInPartitionBeforeSplit`,
the split coordinator computes a route-first split plan, materializes child
stable segments, and atomically updates key-to-segment mapping. Buffered
partition overlays are reassigned to child routes during apply.

Key classes: `segmentindex/split/PartitionStableSplitCoordinator`, `segmentindex/split/PartitionSplitApplyPlan`, `segmentindex/partition/PartitionRuntime`, `segmentindex/mapping/KeyToSegmentMap`.

## 🪦 Delete Semantics (Tombstones)

Deletes write a tombstone value:

- Buffered in the partition overlay like any other update.
- During compaction, tombstones suppress older values and may be dropped if safe.
- Reads treat tombstones as absent.

Key classes:
`segmentindex/core/SegmentIndexImpl#delete`,
`datatype/TypeDescriptor#getTombstone`,
`segment/SegmentSearcher`.

## 💾 Durability and Atomicity

- Transactional writers use a temp file + atomic rename to ensure either the old state or the new state is visible after a crash.
- SegmentIndex `close()` and explicit `flushAndWait()` drive persistence of buffered writes.
- Context logging is not a durability mechanism.

## ⚙️ Configuration Knobs Affecting Writes

- `maxNumberOfKeysInActivePartition` – bounds active mutable overlay size before rotation to an immutable run.
- `maxNumberOfKeysInPartitionBuffer` – bounds buffered keys per partition before local backpressure.
- `maxNumberOfKeysInIndexBuffer` – bounds buffered keys across the whole index before global backpressure.
- `maxNumberOfKeysInSegmentCache` – bounds total in‑segment cache size before compaction/split decisions.
- `maxNumberOfKeysInSegmentChunk` – controls sparse index sampling cadence.
- `maxNumberOfKeysInPartitionBeforeSplit` – split threshold per routed partition.
- `bloomFilter*` – Bloom filter size/hash tuning.
- `diskIoBufferSize` – I/O buffer sizing for on‑disk writers.
- `encoding/decodingChunkFilters` – write/read pipelines (e.g., Snappy, CRC32, magic number).

See: `segmentindex/IndexConfiguration` and `segmentindex/IndexConfigurationBuilder`.

## 🛡️ Integrity Filters on the Write Path

The chunk writer applies a filter pipeline when persisting chunk payloads:

- Magic number writing
- CRC32 computation
- Optional Snappy compression

These produce a self‑describing chunk header and robust payload handling.

Key classes: `chunkstore/ChunkProcessor`, `chunkstore/ChunkFilterMagicNumberWriting`, `chunkstore/ChunkFilterCrc32Writing`, `chunkstore/ChunkFilterSnappyCompress`.

## 🔢 Sequence (Put)

1) `SegmentIndex.put(k,v)` → validate inputs; forbid direct tombstone values
2) Append to WAL (when enabled)
3) Resolve write route via key→segment map
4) Write latest `(k,v)` into active partition mutable layer
5) If active layer crosses threshold:
   - rotate to immutable run
   - schedule background drain
   - apply local/global backpressure if partition/index limits are exceeded

## 🧩 Where to Look in the Code

- SegmentIndex entry points and buffering: `src/main/java/org/hestiastore/index/segmentindex/core/SegmentIndexImpl.java`
- Segment write/merge path: `src/main/java/org/hestiastore/index/segment/*`
- Chunk store and filters: `src/main/java/org/hestiastore/index/chunkstore/*`
- Delta and sorted file writers: `src/main/java/org/hestiastore/index/sorteddatafile/*`

For the read path and on‑disk layout, see the related pages:

- Read Path: `architecture/segmentindex/read-path.md`
- On‑Disk Layout & File Names: `architecture/segment/on-disk-layout.md`
- Filters & Integrity: `architecture/general/filters.md`

## Related Glossary

- [Segment](../general/glossary.md#segment)
- [UniqueCache](../general/glossary.md#uniquecache)
- [Delta Cache](../general/glossary.md#delta-cache)
- [Flush](../general/glossary.md#flush)
- [Compaction](../general/glossary.md#compaction)
- [Split](../general/glossary.md#split)
- [Write Transaction](../general/glossary.md#write-transaction)
- [Filters](../general/glossary.md#filters-chunk-filters)
- [Tombstone](../general/glossary.md#tombstone)
