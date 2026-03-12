# 🗄️ Caching Strategy

HestiaStore uses a few focused caches to deliver read‑after‑write visibility and predictable read latency while keeping memory bounded. This page outlines each layer, how it is populated/evicted, and which configuration knobs control sizing.

For segment internals (delta, Bloom, sparse/scarce structures), use
[Segment Architecture](../segment/index.md). This page describes cache roles at the
SegmentIndex integration level.

## 🎯 Goals

- Read‑after‑write consistency without synchronous disk I/O
- Bound the working set in memory via LRU at the segment layer
- Keep read I/O predictable: avoid random seeks with Bloom filter + sparse index
- Make flush/compact operations deterministic and safe

## 🧱 Layers Overview

- Partition overlay: routed in-memory mutable/immutable layers
  - Classes: `segmentindex/partition/PartitionRuntime`, `segmentindex/partition/RangePartition`
  - Owner: `segmentindex/core/SegmentIndexImpl`
  - Purpose: absorb writes and provide immediate visibility before drain publish

- Segment delta cache: per‑segment overlay of recent writes
  - Classes: `segment/SegmentDeltaCache`, `segment/SegmentDeltaCacheWriter`, `segment/SegmentDeltaCacheController`
  - Purpose: hold sorted updates for a segment between compactions; also backs reads

- Segment data LRU: cache of heavyweight per‑segment objects
  - Classes: `segmentindex/SegmentDataCache` (LRU), values are `segment/SegmentData` (lazy container)
  - Contents: delta cache, Bloom filter, sparse index (scarce index)

- Bloom filter: per‑segment probabilistic set for negative checks
  - Classes: `bloomfilter/*`; created by `segment/SegmentDataSupplier`

- Sparse index ("scarce index"): per‑segment in‑memory snapshot of pointers
  - Classes: `scarceindex/ScarceIndex`, `ScarceIndexSnapshot`

- Key→segment map: max‑key to SegmentId mapping
  - Class: `segmentindex/mapping/KeyToSegmentMap` (TreeMap, persisted to `index.map`)

## ✍️ Write‑Time Caches

### Partition overlay

- On `SegmentIndex.put/delete`, the write is stored in the routed partition's
  active mutable layer.
- New writes replace prior values for the same key within the mutable layer;
  deletes are represented as tombstones.
- Once the active layer crosses threshold, it rotates into immutable runs and
  background drain publishes runs to stable segment storage.

Code:
`segmentindex/core/SegmentIndexImpl#put`,
`segmentindex/core/SegmentIndexImpl#delete`,
`segmentindex/partition/RangePartition#write`,
`segmentindex/core/SegmentIndexImpl#drainPartitions`.

### Segment delta cache

- Flush writes become per‑segment delta files via `SegmentDeltaCacheWriter` (transactional temp file + rename).
- If the segment’s data is currently loaded in memory, the in‑memory delta cache is updated immediately to keep reads fresh.
- Compaction (`SegmentCompacter`) rewrites the segment, then `SegmentDeltaCacheController.clear()` evicts in‑memory delta cache and deletes delta files.

Code: `segment/SegmentDeltaCacheWriter`, `segment/SegmentDeltaCacheController`, `segment/SegmentCompacter`, `segment/SegmentFullWriterTx#doCommit`.

## 📖 Read‑Time Caches

- Top‑level overlay: `SegmentIndex.get(k)` checks routed partition overlay
  first (active mutable then immutable runs).
- Per‑segment overlay: `SegmentDeltaCache` is consulted before the Bloom filter + sparse index path. If it returns a tombstone, the key is absent.
- Heavy objects (Bloom filter, scarce index, delta cache) are obtained via a provider backed by LRU:
  - `segmentindex/SegmentDataCache` holds `segment/SegmentData` instances with an LRU limit; eviction calls `close()` on the container.
  - Providers: `segment/SegmentDataProvider` implementations
    - `segment/SegmentDataProviderLazyLoaded` — lazy local holder

Code:
`segmentindex/core/SegmentIndexImpl#get`,
`segment/SegmentImpl#get`,
`segment/SegmentSearcher`,
`segmentindex/partition/PartitionRuntime`,
`segmentindex/SegmentDataCache`.

## ♻️ Eviction and Lifecycle

- Partition overlay: mutable layer rotates by key budget; immutable runs are
  drained and retired after stable publish.
- SegmentDataCache (LRU of SegmentData): evicts least‑recently‑used segment; eviction closes Bloom filter and clears delta cache via `close()` hook.
- SegmentDeltaCache: cleared and files removed after compaction via `SegmentDeltaCacheController.clear()`; rebuilt on demand from delta files.
- KeyToSegmentMap: persisted via `optionalyFlush()` when updated; survives process restarts by reading `index.map`.

## ⚙️ Configuration Knobs

Index‑level:
- `IndexConfiguration.getMaxNumberOfSegmentsInCache()` — LRU size for `SegmentDataCache`

Per‑segment (via `SegmentConf`, derived from index configuration):
- `maxNumberOfKeysInSegmentCache` — target size for a single delta cache
- `maxNumberOfKeysInActivePartition` — active mutable overlay size before rotation
- `maxNumberOfKeysInPartitionBuffer` — buffered keys allowed per partition before backpressure
- `maxNumberOfKeysInIndexBuffer` — buffered keys allowed across the whole index overlay
- `maxNumberOfKeysInSegmentChunk` — sparse index sampling cadence (affects read scan window)

Bloom filter sizing:
- `bloomFilterIndexSizeInBytes` and `bloomFilterNumberOfHashFunctions`
- `bloomFilterProbabilityOfFalsePositive`

I/O buffering:
- `diskIoBufferSize` — affects memory used by readers/writers across files

See: `segmentindex/IndexConfiguration`, `segment/SegmentConf`.

## 🔥 Warm‑Up Strategies

- Point warm‑up: issue representative `get(key)` calls; this loads the target segments’ Bloom filter and sparse index into the LRU.
- Segment warm‑up: iterate a small range to prime chunk readers and caches.
- Global warm‑up: a bounded `index.getStream(SegmentWindow.limit(N))` over initial segments to seed the LRU without scanning the full dataset.

## 🧭 Observability

- Bloom filter effectiveness and false‑positive rate: `bloomfilter/BloomFilterStats`, accessible via `BloomFilter.getStatistics()`.
- SegmentIndex operation counters (coarse): `segmentindex/Stats` increments on get/put/delete.

## 🛠️ Tuning Guidance

- Throughput‑oriented writes: tune `maxNumberOfKeysInActivePartition`, `maxNumberOfKeysInPartitionBuffer`, and `maxNumberOfKeysInIndexBuffer`; monitor memory and drain latency.
- Read‑heavy workloads touching few segments: increase `maxNumberOfSegmentsInCache` so the working set of segments (Bloom + scarce + delta) stays resident.
- Space‑sensitive deployments: reduce Bloom filter size (may increase false positives and extra reads) or disable compression filters to trade CPU for I/O.
- Latency‑sensitive point lookups: ensure Bloom filter is sized adequately; keep segments’ working set in the LRU; consider slightly smaller `maxNumberOfKeysInSegmentChunk` to narrow the local scan window.

## 🧩 Code Pointers

- Partition overlay and drain: `src/main/java/org/hestiastore/index/segmentindex/core/SegmentIndexImpl.java`,
  `src/main/java/org/hestiastore/index/segmentindex/partition/*`
- Segment caches and providers: `src/main/java/org/hestiastore/index/segmentindex/*SegmentData*`,
  `src/main/java/org/hestiastore/index/segment/SegmentData*`
- LRU cache: `src/main/java/org/hestiastore/index/cache/CacheLru.java`,
  `src/main/java/org/hestiastore/index/cache/CacheLruImpl.java`
- Key→segment map: `src/main/java/org/hestiastore/index/segmentindex/mapping/KeyToSegmentMap.java`

## 🔗 Related Glossary

- [UniqueCache](../general/glossary.md#uniquecache)
- [Delta Cache](../general/glossary.md#delta-cache)
- [SegmentData](../general/glossary.md#segmentdata-and-provider)
- [Bloom Filter](../general/glossary.md#bloom-filter)
- [Sparse Index](../general/glossary.md#sparse-index-scarce-index)
- [Key-to-Segment Map](../general/glossary.md#key-to-segment-map)
