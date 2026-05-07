# Configuration

Use `IndexConfiguration` to define storage behavior, memory limits, type
handling, and selected runtime features for a `SegmentIndex`.

Persisted metadata, runtime tuning, and monitoring fields use canonical
write-path names.

## Start with the minimum configuration

```java
IndexConfiguration<String, String> conf = IndexConfiguration
    .<String, String>builder()
    .identity(identity -> identity
        .name("orders")
        .keyClass(String.class)
        .valueClass(String.class))
    .build();
```

That is enough for many first integrations. Tune only after the basic path
works and you have workload evidence.

## Group advanced settings by domain

For larger configurations, prefer the grouped builder sections so storage
layout, runtime tuning, Bloom filter, WAL, maintenance, and logging settings do
not mix in one long flat chain:

```java
IndexConfiguration<Integer, Integer> conf = IndexConfiguration
    .<Integer, Integer>builder()
    .identity(identity -> identity
        .name("orders")
        .keyClass(Integer.class)
        .valueClass(Integer.class))
    .segment(segment -> segment
        .maxKeys(16_384)
        .chunkKeyLimit(32)
        .cacheKeyLimit(256)
        .cachedSegmentLimit(64)
        .deltaCacheFileLimit(10))
    .writePath(writePath -> writePath
        .segmentWriteCacheKeyLimit(512)
        .maintenanceWriteCacheKeyLimit(8_192)
        .indexBufferedWriteKeyLimit(65_536)
        .segmentSplitKeyThreshold(2_000))
    .bloomFilter(bloom -> bloom
        .indexSizeBytes(1024)
        .hashFunctions(1))
    .maintenance(maintenance -> maintenance
        .segmentThreads(10)
        .indexThreads(10)
        .registryLifecycleThreads(3)
        .backgroundAutoEnabled(true)
        .busyBackoffMillis(5)
        .busyTimeoutMillis(30_000))
    .io(io -> io
        .diskBufferSizeBytes(8192))
    .wal(wal -> wal
        .enabled()
        .durability(WalDurabilityMode.GROUP_SYNC)
        .segmentSizeBytes(64L * 1024L * 1024L)
        .groupSyncDelayMillis(5)
        .groupSyncMaxBatchBytes(1024 * 1024)
        .maxBytesBeforeForcedCheckpoint(512L * 1024L * 1024L)
        .corruptionPolicy(WalCorruptionPolicy.TRUNCATE_INVALID_TAIL)
        .epochSupport(false))
    .logging(logging -> logging
        .contextEnabled(false))
    .build();
```

## Choose a directory implementation

In-memory for tests and short-lived experiments:

```java
Directory directory = new MemDirectory();
```

Filesystem-backed for persistence:

```java
Directory directory = new FsDirectory(new File("/var/lib/hestiastore/orders"));
```

## Configuration areas

### Identity and types

- `identity(...).name()` sets the logical index name for diagnostics and
  logging.
- `identity(...).keyClass()` and `identity(...).valueClass()` declare the key
  and value types.
- `identity(...).keyTypeDescriptor()` and
  `identity(...).valueTypeDescriptor()` are required for non-default custom
  types.

Supported built-in types are the common compact types such as `Integer`,
`Long`, `String`, and `Byte`. Custom types must provide a
`TypeDescriptor` with stable serialization and comparison behavior.

### Segment sizing and cache behavior

- `segment(...).maxKeys()` controls when segments split.
- `segment(...).cacheKeyLimit()` controls how much per-segment data is
  retained in memory.
- `segment(...).cachedSegmentLimit()` controls how many segments stay cached at
  the index level.
- `segment(...).chunkKeyLimit()` controls sparse index granularity.
- `segment(...).deltaCacheFileLimit()` controls how many delta-cache files are
  retained per segment.

These knobs affect memory footprint, lookup cost, and maintenance frequency.
Change them only with representative load testing or benchmark data.

### Write path

Write-path settings control how writes are buffered and when routed segments
become eligible for split maintenance.

- `writePath(...).segmentWriteCacheKeyLimit()` sets the routed segment
  write-cache threshold.
- `writePath(...).maintenanceWriteCacheKeyLimit()` sets the per-segment
  maintenance backlog limit.
- `writePath(...).indexBufferedWriteKeyLimit()` sets the index-wide buffered
  write budget.
- `writePath(...).segmentSplitKeyThreshold()` sets the routed segment split
  eligibility threshold.

### Maintenance and busy-state waiting

Maintenance settings control background workers and retry behavior used when an
operation waits for an internal index state to become available.

- `maintenance(...).backgroundAutoEnabled()` enables automatic background
  maintenance scheduling.
- `maintenance(...).segmentThreads()` sets the segment maintenance thread
  count.
- `maintenance(...).indexThreads()` sets the index maintenance thread count.
- `maintenance(...).registryLifecycleThreads()` sets the registry lifecycle
  thread count.
- `maintenance(...).busyBackoffMillis()` sets how long the retry loop waits
  between two checks of a busy internal state. The default is `5`
  milliseconds.
- `maintenance(...).busyTimeoutMillis()` sets the total retry budget before
  the operation fails instead of waiting longer. The default is `30_000`
  milliseconds.

The same values are stored in `manifest.txt` as `indexBusyBackoffMillis` and
`indexBusyTimeoutMillis`.

### Disk I/O

- `io(...).diskBufferSizeBytes()` sets the buffer size used for disk I/O. The
  default is `8192` bytes.

### Bloom filters

Each segment may use a Bloom filter to accelerate negative lookups.

- `bloomFilter(...).indexSizeBytes()` sets the Bloom filter size
- `bloomFilter(...).hashFunctions()` sets the number of hashes
- `bloomFilter(...).falsePositiveProbability()` tunes expected false positives

Disable Bloom filters by setting:

```java
.bloomFilter(bloom -> bloom.indexSizeBytes(0))
```

### Logging context

`logging(...).contextEnabled()` controls MDC context propagation so logs can carry
the index name.

```xml
<PatternLayout
    pattern="%d{ISO8601} %-5level [%t] index='%X{index.name}' %-C{1.mv}: %msg%n%throwable" />
```

This is useful for multi-index services, but high-throughput workloads should
measure the logging overhead before enabling it everywhere.

### WAL

The write-ahead log is disabled by default. Enable it when you need crash
recovery for acknowledged writes.

- `wal(...).enabled()` enables WAL with defaults unless specific values are
  overridden.
- `wal(...).disabled()` disables WAL explicitly.
- `wal(...).configuration()` copies an existing `IndexWalConfiguration`.
- `wal(...).durability()` selects the durability mode.
- `wal(...).segmentSizeBytes()` controls WAL segment rotation size in bytes.
- `wal(...).groupSyncDelayMillis()` controls how long group sync may delay
  before syncing pending entries.
- `wal(...).groupSyncMaxBatchBytes()` limits the bytes batched before group
  sync.
- `wal(...).maxBytesBeforeForcedCheckpoint()` sets the retained WAL byte budget
  before forced checkpoint/backpressure.
- `wal(...).corruptionPolicy()` controls recovery behavior for invalid WAL
  tails.
- `wal(...).epochSupport()` stores the reserved WAL epoch-support flag.

## Configuration pages by topic

- [Filters](filters.md) for chunk filter setup, provider-backed custom filters,
  and registry wiring
- [Data Types](data-types.md) for custom serialization and comparator contracts
- [Logging](logging.md) for logger configuration
- [Monitoring Console](monitoring-console.md) for monitoring-side configuration

## What can be changed when opening an existing index

Some settings can be overridden when calling `SegmentIndex.open(directory, conf)`.
Treat the index metadata as the source of truth and use overrides only where
the implementation supports runtime-safe reopening.

| Grouped API | Meaning | Reopen override |
| --- | --- | --- |
| `identity().name()` | Logical name of the index | Yes |
| `identity().keyClass()` | Key class | No |
| `identity().valueClass()` | Value class | No |
| `identity().keyTypeDescriptor()` | Key type descriptor | No |
| `identity().valueTypeDescriptor()` | Value type descriptor | No |
| `segment().chunkKeyLimit()` | Keys per segment chunk | No |
| `segment().cacheKeyLimit()` | Keys kept in segment cache | Yes |
| `segment().maxKeys()` | Maximum keys per segment | No |
| `segment().cachedSegmentLimit()` | Cached segments | No on open; use runtime tuning where supported |
| `segment().deltaCacheFileLimit()` | Delta-cache files retained per segment | Yes |
| `writePath().segmentWriteCacheKeyLimit()` | Routed segment write-cache threshold | Yes |
| `writePath().maintenanceWriteCacheKeyLimit()` | Per-segment maintenance backlog limit | Yes |
| `writePath().indexBufferedWriteKeyLimit()` | Index-wide buffered-write budget | No on open; use runtime tuning where supported |
| `writePath().segmentSplitKeyThreshold()` | Routed segment split eligibility threshold | No on open |
| `maintenance().segmentThreads()` | Segment maintenance thread count | Yes |
| `maintenance().indexThreads()` | Index maintenance thread count | Yes |
| `maintenance().registryLifecycleThreads()` | Registry lifecycle thread count | Yes |
| `maintenance().busyBackoffMillis()` | Delay between checks while waiting for a busy internal state | Yes |
| `maintenance().busyTimeoutMillis()` | Total wait budget while waiting for a busy internal state | Yes |
| `maintenance().backgroundAutoEnabled()` | Automatic background maintenance scheduling | Yes |
| `bloomFilter().hashFunctions()` | Bloom filter hash count | No |
| `bloomFilter().indexSizeBytes()` | Bloom filter size | No |
| `bloomFilter().falsePositiveProbability()` | Bloom filter false positive rate | No |
| `io().diskBufferSizeBytes()` | Disk I/O buffer size | Yes |
| `logging().contextEnabled()` | MDC-based context logging | Yes |
| `filters().encodingChunkFilterSpecs()` | Encoding filter pipeline | No |
| `filters().decodingChunkFilterSpecs()` | Decoding filter pipeline | No |
| `wal()` | Write-ahead log configuration | No |

Persisted manifests use the same canonical write-path names as the grouped API.

## Persisted manifest property names

`manifest.txt` stores the same configuration through stable property names.
Some names preserve older partition terminology for compatibility.

| Manifest property | Grouped API |
| --- | --- |
| `keyClass` | `identity().keyClass()` |
| `valueClass` | `identity().valueClass()` |
| `keyTypeDescriptor` | `identity().keyTypeDescriptor()` |
| `valueTypeDescriptor` | `identity().valueTypeDescriptor()` |
| `indexName` | `identity().name()` |
| `contextLoggingEnabled` | `logging().contextEnabled()` |
| `maxNumberOfKeysInSegmentCache` | `segment().cacheKeyLimit()` |
| `segmentWriteCacheKeyLimit` | `writePath().segmentWriteCacheKeyLimit()` |
| `segmentWriteCacheKeyLimitDuringMaintenance` | `writePath().maintenanceWriteCacheKeyLimit()` |
| `indexBufferedWriteKeyLimit` | `writePath().indexBufferedWriteKeyLimit()` |
| `maxNumberOfKeysInSegmentChunk` | `segment().chunkKeyLimit()` |
| `maxNumberOfDeltaCacheFiles` | `segment().deltaCacheFileLimit()` |
| `maxNumberOfKeysInSegment` | `segment().maxKeys()` |
| `segmentSplitKeyThreshold` | `writePath().segmentSplitKeyThreshold()` |
| `maxNumberOfSegmentsInCache` | `segment().cachedSegmentLimit()` |
| `numberOfSegmentMaintenanceThreads` | `maintenance().segmentThreads()` |
| `numberOfIndexMaintenanceThreads` | `maintenance().indexThreads()` |
| `numberOfRegistryLifecycleThreads` | `maintenance().registryLifecycleThreads()` |
| `indexBusyBackoffMillis` | `maintenance().busyBackoffMillis()` |
| `indexBusyTimeoutMillis` | `maintenance().busyTimeoutMillis()` |
| `backgroundMaintenanceAutoEnabled` | `maintenance().backgroundAutoEnabled()` |
| `bloomFilterNumberOfHashFunctions` | `bloomFilter().hashFunctions()` |
| `bloomFilterIndexSizeInBytes` | `bloomFilter().indexSizeBytes()` |
| `bloomFilterProbabilityOfFalsePositive` | `bloomFilter().falsePositiveProbability()` |
| `diskIoBufferSizeInBytes` | `io().diskBufferSizeBytes()` |
| `encodingChunkFilters` | `filters().encodingFilterSpecs()` |
| `decodingChunkFilters` | `filters().decodingFilterSpecs()` |
| `wal.enabled` | `wal().enabled()` / `wal().disabled()` |
| `wal.durabilityMode` | `wal().durability()` |
| `wal.segmentSizeBytes` | `wal().segmentSizeBytes()` |
| `wal.groupSyncDelayMillis` | `wal().groupSyncDelayMillis()` |
| `wal.groupSyncMaxBatchBytes` | `wal().groupSyncMaxBatchBytes()` |
| `wal.maxBytesBeforeForcedCheckpoint` | `wal().maxBytesBeforeForcedCheckpoint()` |
| `wal.corruptionPolicy` | `wal().corruptionPolicy()` |
| `wal.epochSupport` | `wal().epochSupport()` |

Runtime-safe changes can be applied through the runtime configuration with the typed
runtime tuning wrapper:

```java
RuntimeTuningPatch patch = RuntimeTuningPatch.builder()
    .expectedRevision(snapshot.getRevision())
    .maxSegmentsInCache(128)
    .segmentCacheKeyLimit(260_000)
    .writePath(writePath -> writePath
        .segmentWriteCacheKeyLimit(120_000)
        .maintenanceWriteCacheKeyLimit(180_000)
        .indexBufferedWriteKeyLimit(720_000))
    .build();

index.runtimeTuning().applyRuntimeTuning(patch);
```

## Custom data types

If you introduce a custom key or value type, implement
`com.hestiastore.index.datatype.TypeDescriptor` and wire it through the
builder. See [Data Types](data-types.md) for the contract details.
