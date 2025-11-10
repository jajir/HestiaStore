# Read Path

This page explains how reads resolve values with low latency and predictable I/O. It walks through point lookups, range iteration, and the interplay of caches, Bloom filter, and the sparse index, mapped to concrete classes in the codebase.

## High‑Level Flow (Point Lookup)

1. API call: `Index.get(key)`
1. Check the index‑level unique buffer (latest in‑process writes)
1. Locate the target segment using the key→segment map
1. Inside the segment: consult delta cache → Bloom filter → sparse index → local scan

Lookups are read‑after‑write consistent thanks to the in‑memory buffers.

## Entry Point and First‑Level Cache

- `sst/SstIndexImpl#get(K)` does:
  - Check the index‑level `UniqueCache` (holds latest writes prior to flush)
  - If miss, find `SegmentId` via `KeySegmentCache.findSegmentId(key)`
  - Delegate to `Segment.get(key)`

Key classes: `sst/SstIndexImpl.java`, `sst/KeySegmentCache.java`, `cache/UniqueCache.java`.

### Behavior

- Cache hit and non‑tombstone → return value
- Cache hit and tombstone → treat as absent
- Otherwise fall back to the segment path below

## Per‑Segment Read Path

`segment/SegmentImpl#get(key)` uses `SegmentSearcher` with lazily loaded segment data:

1. Delta cache probe: in‑memory map of the segment’s pending updates (merged from delta files). If hit and value not a tombstone → return; tombstone → absent.
1. Bloom filter: `bloomFilter.isNotStored(key)` guards the on‑disk path. If “not stored” → absent.
1. Sparse index ("scarce index"): returns a chunk start position for keys ≥ query.
1. Local scan: within at most N keys (`maxNumberOfKeysInIndexPage`) starting at that chunk, compare keys in ascending order and stop as soon as the target is found or passed.
1. If the sparse index pointed us into the file but no exact key was found, mark a false positive on the Bloom filter for metrics and return absent.

Key classes: `segment/SegmentSearcher.java`, `segment/SegmentIndexSearcher.java`, `scarceindex/ScarceIndex.java`, `bloomfilter/BloomFilter.java`.

## Range Scans and Full Iteration

- `Index.getStream()` and `Index.openSegmentIterator(...)` produce iterators over all data:
  - `sst/SegmentsIterator` chains `Segment.openIterator()` across all segments in order.
  - `segment/SegmentImpl.openIterator()` merges the on‑disk main SST with the segment’s delta cache via `MergeDeltaCacheWithIndexIterator`, skipping tombstones.
  - The per‑segment iterator is wrapped with `EntryIteratorWithLock` using an `OptimisticLock`. If a write changes the segment version mid‑scan, the iterator stops gracefully (no partial records).
  - At the top level, `EntryIteratorRefreshedFromCache` overlays the index‑level unique buffer so that the iterator sees the latest writes even before they’re flushed to disk.

Key classes: `sst/SegmentsIterator.java`, `segment/MergeDeltaCacheWithIndexIterator.java`, `sst/EntryIteratorRefreshedFromCache.java`, `EntryIteratorWithLock.java`, `OptimisticLock.java`.

## Read‑After‑Write Semantics

Two layers provide immediate visibility of recent writes:

- Index‑level `UniqueCache` (pre‑flush) is checked first by `Index.get` and overlaid on iterators.
- Segment delta cache (post‑flush) is kept in memory when loaded; writes to a new delta file also update the in‑memory delta cache when present.

Deletes are represented as tombstones by the value type descriptor. The read path treats a tombstone as “not found”.

## Complexity and I/O Characteristics

- Index‑level cache probe: O(1) hash map
- Segment delta cache probe: O(1) hash map
- Bloom filter probe: O(k) where k is number of hash functions; no I/O
- Sparse index probe: in‑memory list search over a small sample set (fast, cache‑friendly)
- Local scan: sequential read within one chunk window of up to `maxNumberOfKeysInIndexPage` entries
- Iterators: sequential over chunks; minimal seeks due to chunked layout

These choices keep random access bounded and predictable, with sequential I/O for scans.

## Configuration Knobs Affecting Reads

- `maxNumberOfKeysInSegmentChunk` — upper bound of keys per chunk; also the window size for a local scan from the sparse index pointer
- Bloom filter parameters — `numberOfHashFunctions`, `indexSizeInBytes`, `falsePositiveProbability`
- `diskIoBufferSize` — affects chunk and data block I/O buffering
- Encoding/decoding filters — enable CRC32, magic number and optional Snappy compression on read/write paths

See: `sst/IndexConfiguration` and `segment/SegmentConf`.

## Integrity on the Read Path

Decoding applies the inverse of the write pipeline when reading chunks:

- Validate magic number
- Verify CRC32
- Decompress (if Snappy was enabled)

Errors surface as exceptions; partial reads do not corrupt state.

Key classes: `chunkstore/ChunkStoreReaderImpl`, `chunkstore/ChunkFilterMagicNumberValidation`, `chunkstore/ChunkFilterCrc32Validation`, `chunkstore/ChunkFilterSnappyDecompress`.

## Where to Look in the Code

- Point lookup orchestration: `src/main/java/org/hestiastore/index/sst/SstIndexImpl.java`
- Segment search path: `src/main/java/org/hestiastore/index/segment/SegmentSearcher.java`
- Sparse index: `src/main/java/org/hestiastore/index/scarceindex/*`
- Iteration and merging: `src/main/java/org/hestiastore/index/segment/MergeDeltaCacheWithIndexIterator.java`
- Iterator safety: `src/main/java/org/hestiastore/index/EntryIteratorWithLock.java`
 
## Related Glossary

- [Segment](glossary.md#segment)
- [Delta Cache](glossary.md#delta-cache)
- [Bloom Filter](glossary.md#bloom-filter)
- [Sparse Index](glossary.md#sparse-index-scarce-index)
- [UniqueCache](glossary.md#uniquecache)
- [EntryIterator](glossary.md#entryiterator)
- [SegmentWindow](glossary.md#segmentwindow)
