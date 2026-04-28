# Configuration

Use `IndexConfiguration` to define storage behavior, memory limits, type
handling, and selected runtime features for a `SegmentIndex`.

Persisted metadata and monitoring fields may still contain historical
`partition` names for compatibility. Java configuration code uses grouped
sections and canonical write-path names.

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
        .cachedSegmentLimit(64))
    .writePath(writePath -> writePath
        .segmentWriteCacheKeyLimit(512)
        .maintenanceWriteCacheKeyLimit(8_192)
        .indexBufferedWriteKeyLimit(65_536)
        .segmentSplitKeyThreshold(2_000))
    .bloomFilter(bloom -> bloom
        .indexSizeBytes(1024)
        .hashFunctions(1))
    .maintenance(maintenance -> maintenance
        .backgroundAutoEnabled(true))
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

These knobs affect memory footprint, lookup cost, and maintenance frequency.
Change them only with representative load testing or benchmark data.

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
| `segment().cachedSegmentLimit()` | Cached segments | Yes |
| `writePath().segmentWriteCacheKeyLimit()` | Routed segment write-cache threshold | Yes |
| `writePath().maintenanceWriteCacheKeyLimit()` | Per-segment maintenance backlog limit | Yes |
| `writePath().indexBufferedWriteKeyLimit()` | Index-wide buffered-write budget | Yes |
| `writePath().segmentSplitKeyThreshold()` | Routed segment split eligibility threshold | Yes |
| `writePath(...).legacyImmutableRunLimit()` / `runtimeTuning().legacyImmutableRunLimit()` | Legacy compatibility value reserved for runtime tuning and metrics | Yes |
| `bloomFilter().hashFunctions()` | Bloom filter hash count | No |
| `bloomFilter().indexSizeBytes()` | Bloom filter size | No |
| `bloomFilter().falsePositiveProbability()` | Bloom filter false positive rate | No |
| `io().diskBufferSizeBytes()` | Disk I/O buffer size | Yes |
| `logging().contextEnabled()` | MDC-based context logging | Yes |
| `filters().encodingChunkFilterSpecs()` | Encoding filter pipeline | No |
| `filters().decodingChunkFilterSpecs()` | Decoding filter pipeline | No |

Persisted manifests and monitoring payloads may still use historical property
names such as `maxNumberOfKeysInActivePartition`. Treat those names as
compatibility serialization details; Java configuration code should use the
grouped API above.

Runtime-safe changes can be applied through the control plane with the typed
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

index.configurationManagement().applyRuntimeTuning(patch);
```

## Custom data types

If you introduce a custom key or value type, implement
`com.hestiastore.index.datatype.TypeDescriptor` and wire it through the
builder. See [Data Types](data-types.md) for the contract details.
