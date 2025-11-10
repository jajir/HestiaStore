# Write Path

This page describes how a write travels through HestiaStore from the API call to on‑disk structures, highlighting buffering, compaction, and atomicity. It maps directly to the code so you can cross‑check behavior and tune configuration.

## High‑Level Flow

1. API call: `Index.put(key, value)` or `Index.delete(key)`
1. Optional context log append (debug/trace log, not a durability WAL)
1. In‑memory unique write buffer accepts the latest value per key
1. Threshold‑based flush routes buffered writes to target segments
1. Segment delta caches persist sorted updates as transactional files
1. Segment compaction merges delta caches into the main SST + sparse index + bloom filter
1. Optional segment split when size thresholds are exceeded

Writes become durable when flushed to segment files. Closing the index performs a flush.

## Entry Points

- `Index.put(K,V)` and `Index.delete(K)` validate input, update counters, and delegate to the internal implementation.
- Two internal variants exist:
  - Default: `IndexInternalDefault` (non‑synchronized)
  - Synchronized: `IndexInternalSynchronized` (for thread‑safe access)

Key classes: `sst/Index.java`, `sst/IndexInternalDefault.java`.

## Optional Context Log

If `IndexConfiguration.isContextLoggingEnabled()` is true, each write is mirrored to an append‑only log file under `docs` directory using type‑safe serializers. This is intended for observability and debugging, not recovery. The writer rotates on flush or close to start a new file. When disabled, a no‑op log is used.

Key classes: `log/LogImpl`, `log/LogWriter`, `log/LogUnsortedFileWriterImpl`, `log/LoggedKey`.

Notes:

- The log is not used for crash recovery; a real write‑ahead log is on the roadmap.
- Log files are written via transactional temp files and atomic rename on rotate.

## Unique Write Buffer (Index‑Level)

Every `put`/`delete` is first stored in an in‑memory unique cache that holds only the latest value per key. When the buffer exceeds `maxNumberOfKeysInCache`, the index flushes.

- Structure: `UniqueCache` keyed by K with comparator ordering.
- Behavior:
  - New write replaces any previous value for the same key.
  - Reads consult this buffer first (read‑after‑write visibility without disk I/O).
  - Deletes are represented as a tombstone value from the value type descriptor.
- Trigger: `cache.size() > conf.getMaxNumberOfKeysInCache()` calls `flushCache()`.

Key classes: `cache/UniqueCache`, `sst/SstIndexImpl#put`, `sst/SstIndexImpl#delete`.

## Flush and Routing to Segments

On flush, buffered entries are sorted and routed to target segments based on the key‑to‑segment map. Routing is incremental and batched per target segment for locality.

Flow:

1) Sort unique cache entries by key.
2) For each key, find the target segment id via `KeySegmentCache.insertKeyToSegment`.
3) Buffer entries to the current segment; when switching segments, write the batch to that segment’s delta cache and continue.
4) After all entries are written, optionally split segments that exceed size thresholds.
5) Clear the unique buffer, flush the key‑segment map (if changed), and rotate the context log.

Key classes: `sst/CompactSupport`, `sst/KeySegmentCache`, `sst/SegmentSplitCoordinator`.

## Segment Delta Cache Files (Transactional)

Writes land in a segment’s delta cache as sorted key/value files. Each delta file is written transactionally:

- Data is written to `segmentId-delta-XXX.cache.tmp` and atomically renamed on commit.
- Segment properties track counts and delta file numbering.
- If the segment data is currently cached in memory, the delta cache is also updated in‑memory to keep reads fresh.

Key classes: `segment/SegmentDeltaCacheWriter`, `segment/SegmentPropertiesManager`, `sorteddatafile/SortedDataFileWriterTx`.

## On‑Disk Merge (Compaction)

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

## Segment Splitting

When a segment grows beyond `maxNumberOfKeysInSegment`, the split coordinator computes a plan, optionally compacts first, and then splits into two segments. The key‑to‑segment map is updated with the new segment’s max key.

Key classes: `sst/SegmentSplitCoordinator`, `segment/SegmentSplitter`, `segment/SegmentSplitterPlan`, `sst/KeySegmentCache`.

## Delete Semantics (Tombstones)

Deletes write a tombstone value:

- Buffered in the unique cache and delta cache like any other update.
- During compaction, tombstones suppress older values and may be dropped if safe.
- Reads treat tombstones as absent.

Key classes: `sst/SstIndexImpl#delete`, `datatype/TypeDescriptor#getTombstone`, `segment/SegmentSearcher`.

## Durability and Atomicity

- Transactional writers use a temp file + atomic rename to ensure either the old state or the new state is visible after a crash.
- Index `close()` and explicit `flush()` drive persistence of buffered writes.
- Optional context log is not a durability mechanism; it rotates on flush.

## Configuration Knobs Affecting Writes

- `maxNumberOfKeysInCache` – triggers flush of the index‑level buffer.
- `maxNumberOfKeysInSegmentCache` / `…DuringFlushing` – bounds delta cache growth.
- `maxNumberOfKeysInSegmentChunk` – controls sparse index sampling cadence.
- `maxNumberOfKeysInSegment` – split threshold per segment.
- `bloomFilter*` – Bloom filter size/hash tuning.
- `diskIoBufferSize` – I/O buffer sizing for on‑disk writers.
- `encoding/decodingChunkFilters` – write/read pipelines (e.g., Snappy, CRC32, magic number).
- `threadSafe` – choose synchronized index variant.

See: `sst/IndexConfiguration` and `sst/IndexConfigurationBuilder`.

## Integrity Filters on the Write Path

The chunk writer applies a filter pipeline when persisting chunk payloads:

- Magic number writing
- CRC32 computation
- Optional Snappy compression

These produce a self‑describing chunk header and robust payload handling.

Key classes: `chunkstore/ChunkProcessor`, `chunkstore/ChunkFilterMagicNumberWriting`, `chunkstore/ChunkFilterCrc32Writing`, `chunkstore/ChunkFilterSnappyCompress`.

## Sequence (Put)

1) `Index.put(k,v)` → validate inputs; forbid direct tombstone values
2) Optional: append to context log and keep writer open until rotate
3) Buffer latest `(k,v)` into unique cache (replaces any prior value for k)
4) If buffer over threshold → flushCache:
   - Route sorted entries by key to segments
   - For each target segment: write a new delta cache file (transactional)
   - Optionally compact the segment and optionally split if too large
   - Clear unique cache, flush key‑segment map, rotate log

## Where to Look in the Code

- Index entry points and buffering: `src/main/java/org/hestiastore/index/sst/SstIndexImpl.java`
- Segment write/merge path: `src/main/java/org/hestiastore/index/segment/*`
- Chunk store and filters: `src/main/java/org/hestiastore/index/chunkstore/*`
- Delta and sorted file writers: `src/main/java/org/hestiastore/index/sorteddatafile/*`
- Context log (optional): `src/main/java/org/hestiastore/index/log/*`

For the read path and on‑disk layout, see the related pages:

- Read Path: `architecture/read-path.md`
- On‑Disk Layout & File Names: `architecture/on-disk-layout.md`
- Filters & Integrity: `architecture/filters.md`

## Related Glossary

- [Segment](glossary.md#segment)
- [UniqueCache](glossary.md#uniquecache)
- [Delta Cache](glossary.md#delta-cache)
- [Flush](glossary.md#flush)
- [Compaction](glossary.md#compaction)
- [Split](glossary.md#split)
- [Write Transaction](glossary.md#write-transaction)
- [Filters](glossary.md#filters-chunk-filters)
- [Tombstone](glossary.md#tombstone)
- [Context Log](glossary.md#context-log)
