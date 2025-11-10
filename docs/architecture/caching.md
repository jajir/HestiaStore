# Caching Strategy

HestiaStore uses a few focused caches to deliver read‑after‑write visibility and predictable read latency while keeping memory bounded. This page outlines each layer, how it is populated/evicted, and which configuration knobs control sizing.

## Goals

- Read‑after‑write consistency without synchronous disk I/O
- Bound the working set in memory via LRU at the segment layer
- Keep read I/O predictable: avoid random seeks with Bloom filter + sparse index
- Make flush/compact operations deterministic and safe

## Layers Overview

- Index write buffer: in‑memory, unique latest value per key
  - Class: `cache/UniqueCache`
  - Owner: `sst/SstIndexImpl` (top‑level)
  - Purpose: absorb writes and provide immediate visibility before flush

- Segment delta cache: per‑segment overlay of recent writes
  - Classes: `segment/SegmentDeltaCache`, `segment/SegmentDeltaCacheWriter`, `segment/SegmentDeltaCacheController`
  - Purpose: hold sorted updates for a segment between compactions; also backs reads

- Segment data LRU: cache of heavyweight per‑segment objects
  - Classes: `sst/SegmentDataCache` (LRU), values are `segment/SegmentData` (lazy container)
  - Contents: delta cache, Bloom filter, sparse index (scarce index)

- Bloom filter: per‑segment probabilistic set for negative checks
  - Classes: `bloomfilter/*`; created by `segment/SegmentDataSupplier`

- Sparse index ("scarce index"): per‑segment in‑memory snapshot of pointers
  - Classes: `scarceindex/ScarceIndex`, `ScarceIndexSnapshot`

- Key→segment map: max‑key to SegmentId mapping
  - Class: `sst/KeySegmentCache` (TreeMap, persisted to `index.map`)

## Write‑Time Caches

### Index write buffer (UniqueCache)

- On `Index.put/delete`, the write is stored in an index‑level `UniqueCache`.
- Replaces any prior value for the same key; deletes are represented as a tombstone value.
- Triggered flush (`cache.size() > maxNumberOfKeysInCache`) routes sorted writes to target segments and clears the buffer.

Code: `sst/SstIndexImpl#put`, `sst/SstIndexImpl#delete`, `sst/SstIndexImpl#flushCache`, `cache/UniqueCache`.

### Segment delta cache

- Flush writes become per‑segment delta files via `SegmentDeltaCacheWriter` (transactional temp file + rename).
- If the segment’s data is currently loaded in memory, the in‑memory delta cache is updated immediately to keep reads fresh.
- Compaction (`SegmentCompacter`) rewrites the segment, then `SegmentDeltaCacheController.clear()` evicts in‑memory delta cache and deletes delta files.

Code: `segment/SegmentDeltaCacheWriter`, `segment/SegmentDeltaCacheController`, `segment/SegmentCompacter`, `segment/SegmentFullWriterTx#doCommit`.

## Read‑Time Caches

- Top‑level overlay: `Index.get(k)` checks the index write buffer first. Iterators are also overlaid with `EntryIteratorRefreshedFromCache` so scans see most recent writes.
- Per‑segment overlay: `SegmentDeltaCache` is consulted before the Bloom filter + sparse index path. If it returns a tombstone, the key is absent.
- Heavy objects (Bloom filter, scarce index, delta cache) are obtained via a provider backed by LRU:
  - `sst/SegmentDataCache` holds `segment/SegmentData` instances with an LRU limit; eviction calls `close()` on the container.
  - Providers: `segment/SegmentDataProvider` implementations
    - `sst/SegmentDataProviderFromMainCache` — returns/creates from the LRU
    - `segment/SegmentDataProviderSimple` — simple local holder (used in wiring/tests)

Code: `sst/SstIndexImpl#get`, `segment/SegmentImpl#get`, `segment/SegmentSearcher`, `sst/EntryIteratorRefreshedFromCache`, `sst/SegmentDataCache`.

## Eviction and Lifecycle

- UniqueCache (index write buffer): no incremental eviction; cleared on flush.
- SegmentDataCache (LRU of SegmentData): evicts least‑recently‑used segment; eviction closes Bloom filter and clears delta cache via `close()` hook.
- SegmentDeltaCache: cleared and files removed after compaction via `SegmentDeltaCacheController.clear()`; rebuilt on demand from delta files.
- KeySegmentCache: persisted via `optionalyFlush()` when updated; survives process restarts by reading `index.map`.

## Configuration Knobs

Index‑level:
- `IndexConfiguration.getMaxNumberOfKeysInCache()` — size of the index write buffer (triggers flush)
- `IndexConfiguration.getMaxNumberOfSegmentsInCache()` — LRU size for `SegmentDataCache`

Per‑segment (via `SegmentConf`, derived from index configuration):
- `maxNumberOfKeysInSegmentCache` — target size for a single delta cache
- `maxNumberOfKeysInSegmentCacheDuringFlushing` — safety bound while building a delta cache
- `maxNumberOfKeysInSegmentChunk` — sparse index sampling cadence (affects read scan window)

Bloom filter sizing:
- `bloomFilterIndexSizeInBytes` and `bloomFilterNumberOfHashFunctions`
- `bloomFilterProbabilityOfFalsePositive`

I/O buffering:
- `diskIoBufferSize` — affects memory used by readers/writers across files

See: `sst/IndexConfiguration`, `segment/SegmentConf`.

## Warm‑Up Strategies

- Point warm‑up: issue representative `get(key)` calls; this loads the target segments’ Bloom filter and sparse index into the LRU.
- Segment warm‑up: iterate a small range to prime chunk readers and caches.
- Global warm‑up: a bounded `index.getStream(SegmentWindow.limit(N))` over initial segments to seed the LRU without scanning the full dataset.

## Observability

- Bloom filter effectiveness and false‑positive rate: `bloomfilter/BloomFilterStats`, accessible via `BloomFilter.getStatistics()`.
- Index operation counters (coarse): `sst/Stats` increments on get/put/delete.

## Tuning Guidance

- Throughput‑oriented writes: increase `maxNumberOfKeysInCache` to batch more before flushing; monitor memory and flush latency.
- Read‑heavy workloads touching few segments: increase `maxNumberOfSegmentsInCache` so the working set of segments (Bloom + scarce + delta) stays resident.
- Space‑sensitive deployments: reduce Bloom filter size (may increase false positives and extra reads) or disable compression filters to trade CPU for I/O.
- Latency‑sensitive point lookups: ensure Bloom filter is sized adequately; keep segments’ working set in the LRU; consider slightly smaller `maxNumberOfKeysInSegmentChunk` to narrow the local scan window.

## Code Pointers

- Index write buffer: `src/main/java/org/hestiastore/index/sst/SstIndexImpl.java`
- Segment caches and providers: `src/main/java/org/hestiastore/index/sst/*SegmentData*`, `src/main/java/org/hestiastore/index/segment/SegmentData*`
- LRU cache: `src/main/java/org/hestiastore/index/cache/CacheLru.java`
- Key→segment map: `src/main/java/org/hestiastore/index/sst/KeySegmentCache.java`

## Related Glossary

- [UniqueCache](glossary.md#uniquecache)
- [Delta Cache](glossary.md#delta-cache)
- [SegmentData](glossary.md#segmentdata-and-provider)
- [Bloom Filter](glossary.md#bloom-filter)
- [Sparse Index](glossary.md#sparse-index-scarce-index)
- [Key-to-Segment Map](glossary.md#key-to-segment-map)
