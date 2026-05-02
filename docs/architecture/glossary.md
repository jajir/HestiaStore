# Glossary

Concise definitions of terms used across HestiaStore’s architecture, with links and code pointers.

## Backpressure
Controlled retry/throttling when a routed segment, registry lookup, or route
topology lease is temporarily `BUSY`, or when WAL retention pressure requires
checkpoint progress before more writes are accepted. Code:
`segmentindex/core/operations/IndexOperationCoordinator.java`,
`segmentindex/core/split/SplitPolicyCoordinator.java`,
`segmentindex/wal/WalRuntime.java`.

## Bloom Filter
Per‑segment probabilistic set that quickly proves absence and reduces on‑disk probes; rebuilt during compaction. Code: `bloomfilter/*`.

## Bounded Buffering
Capacity model that bounds segment-local write-cache and maintenance backlog
growth so ingest memory stays predictable under load. Some settings still keep
historical `partition` names for compatibility. Code:
`segmentindex/IndexWritePathConfiguration.java`,
`segmentindex/IndexRuntimeTuningConfiguration.java`,
`segmentindex/runtimeconfiguration/RuntimeSettingKey.java`,
`segment/SegmentRuntimeLimits.java`.

## Chunk
Fixed‑cell payload plus a small header (magic, version, payload length, CRC, flags). Filters may transform payload on write and are inverted on read. Code: `chunkstore/Chunk*.java`.

## Compaction
Segment rewrite that merges main SST with delta caches into fresh `vNN-index.sst`, `vNN-scarce.sst`, and `vNN-bloom-filter.bin` files, then clears delta caches. Code: `segment/SegmentCompacter.java`, `segment/SegmentFullWriter*.java`.

## Consistency Checker
Utilities to verify sortedness and segment/map coherence after unexpected shutdowns; can repair certain metadata issues. Code: `segmentindex/IndexConsistencyChecker.java`, `segment/SegmentConsistencyChecker.java`.

## Delta Cache
Per‑segment overlay of recent updates, materialized as sorted `.cache` files and an in‑memory `UniqueCache` when loaded. Code: `segment/SegmentDeltaCache*.java`.

## Directory (Abstraction)
File I/O backend (FS, memory, zip) providing readers/writers and atomic rename. Code: `directory/*`.

## Drain
Historical term from the removed partition-overlay runtime. In the current
direct-to-segment model, maintenance flushes segment write-cache snapshots into
delta files and compaction rewrites stable segment files. Compatibility metrics
still keep some legacy `drain*` names.

## Durability
Persistence guarantee for acknowledged writes. With WAL enabled, durability mode controls acknowledgement timing (`ASYNC`, `GROUP_SYNC`, `SYNC`); with WAL disabled, explicit maintenance completion (`maintenance().flushAndWait()`/close) is the durability boundary. Code: `segmentindex/WalDurabilityMode.java`, `segmentindex/maintenance/SegmentIndexMaintenance#flushAndWait()`, `index/GuardedWriteTransaction.java`.

## Entry
Immutable key/value pair used across iterators and writers. Code: `index/Entry.java`.

## EntryIterator
Forward iterator over entries; variants exist for merging overlays and for safe iteration under writes (optimistic lock). Code: `index/EntryIterator.java`, `segment/MergeDeltaCacheWithIndexIterator.java`, `index/EntryIteratorWithLock.java`.

## Filters (Chunk Filters)
Pluggable transformations applied to chunk payloads on write and inverted on read (magic number, CRC32, Snappy, XOR). Configured per index. Code: `chunkstore/ChunkFilter*.java`; config via `segmentindex/IndexConfigurationBuilder`.

## Flush
Schedules or awaits per-segment persistence of write-cache snapshots and then
flushes `index.map`. `maintenance().flushAndWait()` also waits for split settlement and WAL
checkpoint when WAL is enabled. Code:
`segmentindex/core/maintenance/MaintenanceService.java`,
`segmentindex/core/maintenance/MaintenanceServiceImpl.java`,
`segmentindex/mapping/KeyToSegmentMap.java`.

## Hot Partition
Routed key range that receives a disproportionately large share of reads or
writes compared with the rest of the index. Hot routes are where split policy,
segment write-cache pressure, and maintenance latency matter most. Code:
`segmentindex/core/split/SplitPolicyCoordinator.java`,
`segmentindex/core/segmentaccess/DefaultSegmentAccessService.java`.

## Ingest (Index Ingest)
Index write path where `put` and `delete` append to WAL first when enabled,
resolve the current route, and write directly into the target stable segment.
Read-after-write visibility is provided by the segment write cache. Code:
`segmentindex/core/operations/IndexOperationCoordinator.java`,
`segmentindex/core/segmentaccess/DefaultSegmentAccessService.java`.

## Key-to-Segment Map
Global sorted map of max key → SegmentId that routes lookups and stable publish targets. Persisted as `index.map`. Code: `segmentindex/mapping/KeyToSegmentMap.java`.

## Segment Topology
Runtime route table that tracks `ACTIVE`, `DRAINING`, and `RETIRED` route
states plus in-flight route leases. It is rebuilt from KeyToSegmentMap
snapshots and is not persisted independently. Code:
`segmentindex/core/topology/SegmentTopology.java`.

## Logging Context
Optional MDC enrichment that sets `index.name` for log correlation when enabled.

## Main SST
On‑disk, chunked Sorted String Table containing sorted key/value entries for a segment. Code: `chunkentryfile/*`, `chunkstore/*`.

## Overlay
Historical term for the removed partition-overlay runtime. Current
`SegmentIndex` reads and writes go directly through routed stable segments.

## Orphaned Segment
Segment directory that exists on disk but is not referenced by persisted routing metadata (`index.map`) and is not a pending split source. Cleanup removes these leftovers during recovery/consistency handling. Code: `segmentindex/core/SegmentIndexImpl#cleanupOrphanedSegmentDirectories()`, `segmentindex/core/SegmentIndexImpl#deleteOrphanedSegmentDirectory()`.

## Recovery
Startup and repair path that restores stable metadata, rebuilds routing,
replays WAL records above checkpoint through the direct write path, and handles
invalid tails according to corruption policy before returning to ready state.
Code: `segmentindex/wal/WalRuntime.java`,
`segmentindex/core/operations/IndexOperationCoordinator.java`,
`segmentindex/IndexConsistencyChecker.java`.

## Segment
Bounded shard of the index stored on disk with its own files: main SST (`vNN-index.sst`), sparse index (`vNN-scarce.sst`), Bloom filter (`vNN-bloom-filter.bin`), `manifest.txt`, optional delta caches (`vNN-delta-NNNN.cache`), and `.lock`. See also: On‑Disk Layout. Code: `segment/*`, `segmentindex/SegmentRegistry.java`.

## SegmentData and Provider
 Lazy containers and providers for per‑segment heavyweight structures (delta cache, Bloom, sparse index). Often cached via LRU. Code: `segment/SegmentData*.java`, `segmentindex/SegmentDataCache.java`.

## SegmentId
Stable integer id rendered as `segment-00000`, used to name per‑segment files. Code: `segment/SegmentId.java`.

## SegmentWindow
Offset/limit window for streaming across segments, analogous to SQL OFFSET/LIMIT. Code: `segmentindex/SegmentWindow.java`.

## Sparse Index (Scarce Index)
Per‑segment, sorted sample of keys that points to chunk start positions in the main SST to bound local scans. Code: `scarceindex/*`.

## Split
Maintenance operation that replaces one routed segment range with child ranges
when split policy is met; the route map is remapped atomically after child
segments are materialized. Code:
`segmentindex/core/split/RouteSplitCoordinator.java`,
`segmentindex/core/split/SplitPolicyCoordinator.java`.

## Split Policy
Background decision logic that identifies routed ranges worth splitting and schedules the work with cooldown, hysteresis, and in-flight guards so the system avoids split thrash. Code: `segmentindex/core/split/SplitPolicyCoordinator.java`, `segmentindex/core/session/SegmentIndexImpl.java`.

## Split Procedure
Route-first split flow: compute the split boundary from the parent stable
snapshot, materialize lower/upper child stable segments, atomically apply the
route-map update, and retire the parent segment. Code:
`segmentindex/core/split/RouteSplitCoordinator.java`,
`segmentindex/core/split/RouteSplitPlan.java`.

## Split-heavy Workload
Workload pattern or benchmark mode that intentionally drives frequent split
candidates, typically by growing a routed keyspace under load while reads and
writes continue. It is useful for validating autonomous split policy, child
publish flow, and read/write behavior during repeated remapping. Code:
`benchmark/segmentindex/SegmentIndexMixedDrainBenchmark.java`,
`segmentindex/core/split/SplitPolicyCoordinator.java`,
`segmentindex/core/split/RouteSplitCoordinator.java`.

## Stats
Simple counters for get/put/delete to observe workload shape. Code: `segmentindex/Stats.java`.

## Thrash (Thrashing)
Pathological churn where the system repeatedly retries, reloads, evicts, splits, or rescans the same hot range/cache entries without making proportional forward progress. In this project the term is typically used for split thrash or cache thrash, for example when a hot routed segment keeps re-entering maintenance/scheduling pressure or when registry cache entries are repeatedly unloaded and loaded again under pressure. Code: `segmentindex/core/session/SegmentIndexImpl.java`, `segmentindex/core/split/SplitPolicyCoordinator.java`, `segmentregistry/SegmentRegistryCache.java`.

## Tombstone
Special value denoting deletion; read path treats it as absent and compaction drops obsolete values. Provided by the value type descriptor. Code: `datatype/TypeDescriptor#getTombstone()`, used in `segmentindex/core/session/SegmentIndexImpl#delete()`.

## UniqueCache
In-memory map that keeps only the latest value per key. Used inside segment
write-cache and delta-cache implementations. Code: `cache/UniqueCache*.java`.

## WAL (Write‑Ahead Log)
Per-index append log in `wal/` that records `PUT`/`DELETE` operations with LSN and checksum before apply. It provides replay, checkpointing, segment rotation, and invalid-tail handling according to policy. Code: `segmentindex/IndexWalConfiguration.java`, `segmentindex/wal/WalRuntime.java`, `segmentindex/wal/WalTool.java`.

## Write Transaction
Pattern that enforces open → close → commit, guaranteeing atomic file replacement. Code: `index/GuardedWriteTransaction.java`, `index/WriteTransaction.java`.
