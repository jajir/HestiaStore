# Glossary

Concise definitions of terms used across HestiaStore’s architecture, with links and code pointers.

## Segment
Bounded shard of the index stored on disk with its own files: main SST (`.index`), sparse index (`.scarce`), Bloom filter (`.bloom-filter`), properties, and optional delta caches. See also: On‑Disk Layout. Code: `segment/*`, `sst/SegmentRegistry.java`.

## SegmentId
Stable integer id rendered as `segment-00000`, used to name per‑segment files. Code: `segment/SegmentId.java`.

## Key-to-Segment Map
Global sorted map of max key → SegmentId that routes lookups and flushes. Persisted as `index.map`. Code: `sst/KeySegmentCache.java`.

## Main SST
On‑disk, chunked Sorted String Table containing sorted key/value entries for a segment. Code: `chunkentryfile/*`, `chunkstore/*`.

## Chunk
Fixed‑cell payload plus a small header (magic, version, payload length, CRC, flags). Filters may transform payload on write and are inverted on read. Code: `chunkstore/Chunk*.java`.

## Delta Cache
Per‑segment overlay of recent updates, materialized as sorted `.cache` files and an in‑memory `UniqueCache` when loaded. Code: `segment/SegmentDeltaCache*.java`.

## UniqueCache
In‑memory map that keeps only the latest value per key. Used at the index‑level write buffer and inside the delta overlay. Code: `cache/UniqueCache*.java`.

## Flush
Drains the index‑level write buffer, routes entries to per‑segment delta caches, and updates `index.map`. Code: `sst/SstIndexImpl#flush()`, `sst/CompactSupport.java`.

## Compaction
Segment rewrite that merges main SST with delta caches into fresh `.index`, `.scarce`, and `.bloom-filter` files, then clears delta caches. Code: `segment/SegmentCompacter.java`, `segment/SegmentFullWriter*.java`.

## Split
When a segment grows beyond `maxNumberOfKeysInSegment`, it is split into two; `index.map` is updated with a new max key and SegmentId. Code: `sst/SegmentSplitCoordinator.java`, `segment/SegmentSplitter*.java`.

## Sparse Index (Scarce Index)
Per‑segment, sorted sample of keys that points to chunk start positions in the main SST to bound local scans. Code: `scarceindex/*`.

## Bloom Filter
Per‑segment probabilistic set that quickly proves absence and reduces on‑disk probes; rebuilt during compaction. Code: `bloomfilter/*`.

## Filters (Chunk Filters)
Pluggable transformations applied to chunk payloads on write and inverted on read (magic number, CRC32, Snappy, XOR). Configured per index. Code: `chunkstore/ChunkFilter*.java`; config via `sst/IndexConfigurationBuilder`.

## Tombstone
Special value denoting deletion; read path treats it as absent and compaction drops obsolete values. Provided by the value type descriptor. Code: `datatype/TypeDescriptor#getTombstone()`, used in `sst/SstIndexImpl#delete()`.

## Entry
Immutable key/value pair used across iterators and writers. Code: `index/Entry.java`.

## EntryIterator
Forward iterator over entries; variants exist for merging overlays and for safe iteration under writes (optimistic lock). Code: `index/EntryIterator.java`, `segment/MergeDeltaCacheWithIndexIterator.java`, `index/EntryIteratorWithLock.java`.

## Write Transaction
Pattern that enforces open → close → commit, guaranteeing atomic file replacement. Code: `index/GuardedWriteTransaction.java`, `index/WriteTransaction.java`.

## Directory (Abstraction)
File I/O backend (FS, memory, zip) providing readers/writers and atomic rename. Code: `directory/*`.

## SegmentData and Provider
Lazy containers and providers for per‑segment heavyweight structures (delta cache, Bloom, sparse index). Often cached via LRU. Code: `segment/SegmentData*.java`, `sst/SegmentDataCache.java`, `sst/SegmentDataProviderFromMainCache.java`.

## SegmentWindow
Offset/limit window for streaming across segments, analogous to SQL OFFSET/LIMIT. Code: `sst/SegmentWindow.java`.

## Stats
Simple counters for get/put/delete to observe workload shape. Code: `sst/Stats.java`.

## Consistency Checker
Utilities to verify sortedness and segment/map coherence after unexpected shutdowns; can repair certain metadata issues. Code: `sst/IndexConsistencyChecker.java`, `segment/SegmentConsistencyChecker.java`.

## Context Log
Optional append‑only log of operations for observability (not a recovery WAL). Code: `log/*`.
