# Performance Model & Sizing

This page summarizes how HestiaStore achieves high throughput and predictable
latency, and how to size the main knobs. All claims map to code so you can
verify behavior.

Segment-specific internals referenced here are centralized in
[Segment Architecture](../segment/index.md).

## Mental Model (Hot Paths)

- Put/Delete:
  - Route lookup is an in-memory map operation.
  - `Segment.put(...)` updates the target segment write cache in O(1).
  - Flush writes frozen write-cache snapshots into per-segment delta files.
  - Optional compaction merges delta files into the main SST.

- Get (negative):
  - O(k) Bloom probe (k = hash functions), no disk I/O when filter says “absent”.

- Get (positive):
  - Locate target segment via key→segment map (in‑memory TreeMap ceiling lookup).
  - Seek into `vNN-index.sst` by sparse index pointer, then bounded local scan of at most `maxNumberOfKeysInSegmentChunk` entries in ascending order. Typically one chunk read.

## I/O Patterns and Amplification

- Sequential writes: delta files and SST chunks append sequentially via transactional writers (`*.tmp` + rename).
- Sequential reads: positive get reads one chunk and scans ≤ N keys (N = `maxNumberOfKeysInSegmentChunk`).
- Negative reads: avoid disk I/O via Bloom filter unless false positive.
- Alignment and block size:
  - Chunk store uses fixed 16‑byte cells with data blocks sized by `diskIoBufferSize` (divisible by 1024). Payloads are padded to whole cells for easy positioning.
  - Code: `chunkstore/CellPosition.java`, `datablockfile/DataBlockSize.java`, `Vldtn#requireIoBufferSize`.

## Key Knobs (What They Do)

- `maxNumberOfKeysInSegmentChunk` (sparse index cadence)
  - Lower ⇒ smaller local scan window (read latency) with more sparse‑index entries; slightly more write work during compaction.

- Bloom filter sizing: `bloomFilterIndexSizeInBytes`, `bloomFilterNumberOfHashFunctions`, or target probability
  - From `BloomFilterBuilder`: m = −(n ln p)/(ln2)^2, k ≈ m/n·ln2. Larger m lowers false positives and I/O on negative lookups at the cost of RAM and disk for the filter.
  - Code: `bloomfilter/BloomFilterBuilder.java`.

- `maxNumberOfSegmentsInCache` (SegmentData LRU)
  - Number of segments whose Bloom + sparse index + delta cache can be resident. Too small ⇒ thrash; too large ⇒ memory waste.

- `diskIoBufferSize`
  - Sets data‑block size for chunk store and buffers for file readers/writers. Choose 4–64 KiB depending on device. Must be divisible by 1024.

- Encoding/Decoding filters (CRC, magic, Snappy, XOR)
  - Snappy reduces I/O on compressible values at CPU cost. CRC + magic are lightweight integrity guards and on by default.

- Context logging (`isContextLoggingEnabled`)
  - Adds MDC setup/teardown per operation so logs can include `index.name`.

## Memory Sizing

- Per-segment write cache: bounded by the routed write-cache threshold and the
  segment maintenance backlog.
- Per-segment delta cache (in memory): when a segment is loaded, delta files
  are folded into a `UniqueCache`. Upper bound approximates the number of
  unique keys across delta files.
- Bloom filter: fully memory‑mapped in RAM when present; `indexSizeInBytes` bytes per segment plus metadata. Code: `bloomfilter/BloomFilterImpl.java`.
- SegmentData LRU: holds delta cache + Bloom + scarce index for up to `maxNumberOfSegmentsInCache` segments; evictions call `close()` to free memory.

## CPU Sizing

- Put path: hashing and HashMap work; occasional sort on flush (parallel sort over entries) and CRC/magic/Snappy filters on compaction.
- Get path: a few compares, at most N key compares during the bounded scan, optional Snappy decompression on read.
- Enabling context logging adds a small per‑operation MDC overhead.

## Practical Tuning Recipes

- Write‑heavy ingestion:
  - Consider enabling Snappy if values are highly compressible and I/O bound.
  - Keep `maxNumberOfKeysInSegmentChunk` moderate (e.g., 512–2048) to keep sparse index size reasonable during compaction.

- Read‑latency sensitive point lookups:
  - Ensure Bloom filters are sized adequately (lower false positive rate with larger `indexSizeInBytes`).
  - Reduce `maxNumberOfKeysInSegmentChunk` to shrink the local scan window.
  - Increase `maxNumberOfSegmentsInCache` so hot segments stay resident.

- Mixed workloads:
  - Start with defaults; adjust Bloom size and segment LRU to fit your hot set; validate with counters and filter stats.

## Observability and Validation

- Bloom stats: `BloomFilter.getStatistics()` reports avoided disk accesses and false‑positive rate. Code: `bloomfilter/BloomFilterStats`.
- Operation counters: `segmentindex/Stats` exposes get/put/delete counts (logged on close in `SegmentIndexImpl#doClose`).
- Consistency: after unexpected shutdown, run `SegmentIndex.checkAndRepairConsistency()`; optionally `compact()` to reclaim locality.

## Code Pointers

- Routed direct writes and stable maintenance:
  `src/main/java/org/hestiastore/index/segmentindex/core/DirectSegmentWriteCoordinator.java`,
  `src/main/java/org/hestiastore/index/segmentindex/core/IndexMaintenanceCoordinator.java`
- Read path bounds: `src/main/java/org/hestiastore/index/segment/SegmentSearcher.java`,
  `src/main/java/org/hestiastore/index/segment/SegmentIndexSearcher.java`
- Bloom filter: `src/main/java/org/hestiastore/index/bloomfilter/*`
- Chunked I/O and filters: `src/main/java/org/hestiastore/index/chunkstore/*`
- Segment sizing/splitting:
  `src/main/java/org/hestiastore/index/segmentindex/split/RouteSplitCoordinator.java`,
  `src/main/java/org/hestiastore/index/segmentindex/core/BackgroundSplitCoordinator.java`

## Related Glossary

- [Main SST](../glossary.md#main-sst)
- [Sparse Index](../glossary.md#sparse-index-scarce-index)
- [Bloom Filter](../glossary.md#bloom-filter)
- [UniqueCache](../glossary.md#uniquecache)
- [Delta Cache](../glossary.md#delta-cache)
- [Compaction](../glossary.md#compaction)
- [Write Transaction](../glossary.md#write-transaction)
