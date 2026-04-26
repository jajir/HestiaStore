# Read Path

This page explains how reads resolve values with low latency and predictable
I/O. It walks through point lookups, range iteration, and the interplay of
segment-local caches, Bloom filter, and the sparse index, mapped to concrete
classes in the codebase.

Segment-internal details are centralized in
[Segment Architecture](../segment/index.md). This page focuses on the
SegmentIndex-level orchestration path.

## High‑Level Flow (Point Lookup)

1. API call: `SegmentIndex.get(key)`
1. Resolve the target segment using the key→segment map and `SegmentTopology`
1. Read directly from the routed segment
1. Inside the segment: consult write cache → delta cache → Bloom filter →
   sparse index → local scan

Lookups are read-after-write consistent because each `Segment` checks its own
in-memory write cache before falling back to stable on-disk state.

## Entry Point and Routing

- `segmentindex/core/SegmentIndexImpl#get(K)` delegates to
  `SegmentIndexPointOperationFacade`, which runs the point lookup under index
  state and close-safety checks.
- `IndexOperationCoordinator#get(K)` uses `SegmentAccessService` to resolve the
  current route, acquire a `SegmentTopology` lease, and load the segment.
- The loaded segment handles the local `get(K)` lookup.

Key classes:
`segmentindex/core/session/SegmentIndexImpl.java`,
`segmentindex/core/session/SegmentIndexPointOperationFacade.java`,
`segmentindex/core/operations/IndexOperationCoordinator.java`,
`segmentindex/core/segmentaccess/SegmentAccessService.java`,
`segmentindex/mapping/KeyToSegmentMap.java`.

## Per‑Segment Read Path

`segment/SegmentImpl#get(key)` uses `SegmentSearcher` with lazily loaded
segment data:

1. Write cache probe: the segment's latest in-memory writes are checked first.
1. Delta cache probe: in-memory map of pending updates merged from delta files.
   If hit and value not a tombstone → return; tombstone → absent.
1. Bloom filter: `bloomFilter.isNotStored(key)` guards the on-disk path. If
   “not stored” → absent.
1. Sparse index ("scarce index"): returns a chunk start position for keys ≥
   query.
1. Local scan: within at most N keys (`maxNumberOfKeysInSegmentChunk`) starting
   at that chunk, compare keys in ascending order and stop as soon as the
   target is found or passed.
1. If the sparse index pointed into the file but no exact key was found, mark
   a false positive on the Bloom filter for metrics and return absent.

Key classes:
`segment/SegmentSearcher.java`,
`segment/SegmentIndexSearcher.java`,
`scarceindex/ScarceSegmentIndex.java`,
`bloomfilter/BloomFilter.java`.

## Range Scans and Full Iteration

- `SegmentIndex.getStream()` and `SegmentIndex.openSegmentIterator(...)`
  produce iterators over routed segments in order.
- `DirectSegmentCoordinator.openWindowIterator(...)` opens iterators
  against a route snapshot.
- `segment/SegmentImpl.openIterator()` merges the on-disk main SST with the
  segment's delta cache via `MergeDeltaCacheWithIndexIterator`, skipping
  tombstones.
- The per-segment iterator is wrapped with `EntryIteratorWithLock` using an
  `OptimisticLock`. If a write changes the segment version mid-scan, the
  iterator stops gracefully.
- For `FULL_ISOLATION`, the index retries iterator open if the route map
  changes while the iterator is being opened.

Key classes:
`segmentindex/core/streaming/DirectSegmentCoordinator.java`,
`segmentindex/core/streaming/SegmentsIterator.java`,
`segment/MergeDeltaCacheWithIndexIterator.java`,
`EntryIteratorWithLock.java`,
`OptimisticLock.java`.

## Read‑After‑Write Semantics

Read-after-write is segment-local:

- `SegmentIndex` routes the request to one stable segment.
- `Segment.get(key)` checks the segment write cache before reading stable data.
- Tombstones are treated as “not found”.

## Complexity and I/O Characteristics

- Route lookup: in-memory map lookup over routed segments
- Segment write/delta cache probe: O(1) hash map
- Bloom filter probe: O(k) where k is number of hash functions; no I/O
- Sparse index probe: in-memory list search over a small sample set
- Local scan: sequential read within one chunk window of up to
  `maxNumberOfKeysInSegmentChunk` entries
- Iterators: sequential over chunks; minimal seeks due to chunked layout

These choices keep random access bounded and predictable, with sequential I/O
for scans.

## Configuration Knobs Affecting Reads

- `maxNumberOfKeysInSegmentChunk` — upper bound of keys per chunk; also the
  window size for a local scan from the sparse-index pointer
- Bloom filter parameters — `numberOfHashFunctions`, `indexSizeInBytes`,
  `falsePositiveProbability`
- `diskIoBufferSize` — affects chunk and data block I/O buffering
- Encoding/decoding filters — enable CRC32, magic number and optional Snappy
  compression on read/write paths

See: `segmentindex/IndexConfiguration` and `segment/SegmentConf`.

## Integrity on the Read Path

Decoding applies the inverse of the write pipeline when reading chunks:

- Validate magic number
- Verify CRC32
- Decompress (if Snappy was enabled)

Errors surface as exceptions; partial reads do not corrupt state.

Key classes:
`chunkstore/ChunkStoreReaderImpl`,
`chunkstore/ChunkFilterMagicNumberValidation`,
`chunkstore/ChunkFilterCrc32Validation`,
`chunkstore/ChunkFilterSnappyDecompress`.

## Where to Look in the Code

- Point lookup orchestration:
  `src/main/java/org/hestiastore/index/segmentindex/core/session/SegmentIndexImpl.java`
- Direct routed reads:
  `src/main/java/org/hestiastore/index/segmentindex/core/streaming/DirectSegmentCoordinator.java`
- Segment search path:
  `src/main/java/org/hestiastore/index/segment/SegmentSearcher.java`
- Sparse index: `src/main/java/org/hestiastore/index/scarceindex/*`
- Iteration and merging:
  `src/main/java/org/hestiastore/index/segment/MergeDeltaCacheWithIndexIterator.java`
- Iterator safety:
  `src/main/java/org/hestiastore/index/EntryIteratorWithLock.java`

## Related Glossary

- [Segment](../glossary.md#segment)
- [Delta Cache](../glossary.md#delta-cache)
- [Bloom Filter](../glossary.md#bloom-filter)
- [Sparse Index](../glossary.md#sparse-index-scarce-index)
- [EntryIterator](../glossary.md#entryiterator)
- [SegmentWindow](../glossary.md#segmentwindow)
