# Configuration

Use `IndexConfiguration` to define storage behavior, memory limits, type
handling, and selected runtime features for a `SegmentIndex`.

## Start with the minimum configuration

```java
IndexConfiguration<String, String> conf = IndexConfiguration
    .<String, String>builder()
    .withKeyClass(String.class)
    .withValueClass(String.class)
    .withName("orders")
    .build();
```

That is enough for many first integrations. Tune only after the basic path
works and you have workload evidence.

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

- `withName()` sets the logical index name for diagnostics and logging.
- `withKeyClass()` and `withValueClass()` declare the key and value types.
- `withKeyTypeDescriptor()` and `withValueTypeDescriptor()` are required for
  non-default custom types.

Supported built-in types are the common compact types such as `Integer`,
`Long`, `String`, and `Byte`. Custom types must provide a
`TypeDescriptor` with stable serialization and comparison behavior.

### Segment sizing and cache behavior

- `withMaxNumberOfKeysInSegment()` controls when segments split.
- `withMaxNumberOfKeysInSegmentCache()` controls how much per-segment data is
  retained in memory.
- `withMaxNumberOfSegmentsInCache()` controls how many segments stay cached at
  the index level.
- `withMaxNumberOfKeysInSegmentIndexPage()` controls sparse index granularity.

These knobs affect memory footprint, lookup cost, and maintenance frequency.
Change them only with representative load testing or benchmark data.

### Bloom filters

Each segment may use a Bloom filter to accelerate negative lookups.

- `withBloomFilterIndexSizeInBytes()` sets the Bloom filter size
- `withBloomFilterNumberOfHashFunctions()` sets the number of hashes
- `withBloomFilterProbabilityOfFalsePositive()` tunes expected false positives

Disable Bloom filters by setting:

```java
.withBloomFilterIndexSizeInBytes(0)
```

### Logging context

`withContextLoggingEnabled()` controls MDC context propagation so logs can carry
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

| Name | Meaning | Reopen override |
| --- | --- | --- |
| `indexName` | Logical name of the index | Yes |
| `keyClass` | Key class | No |
| `valueClass` | Value class | No |
| `keyTypeDescriptor` | Key type descriptor | No |
| `valueTypeDescriptor` | Value type descriptor | No |
| `maxNumberOfKeysInSegmentIndexPage` | Keys per segment index page | No |
| `maxNumberOfKeysInSegmentCache` | Keys kept in segment cache | Yes |
| `maxNumberOfKeysInActivePartition` | Active partition overlay size | Yes |
| `maxNumberOfImmutableRunsPerPartition` | Immutable runs per partition | Yes |
| `maxNumberOfKeysInPartitionBuffer` | Buffered keys per partition | Yes |
| `maxNumberOfKeysInIndexBuffer` | Buffered keys across index | Yes |
| `maxNumberOfKeysInPartitionBeforeSplit` | Split eligibility threshold | Yes |
| `maxNumberOfKeysInSegment` | Maximum keys per segment | No |
| `maxNumberOfSegmentsInCache` | Cached segments | Yes |
| `bloomFilterNumberOfHashFunctions` | Bloom filter hash count | No |
| `bloomFilterIndexSizeInBytes` | Bloom filter size | No |
| `bloomFilterProbabilityOfFalsePositive` | Bloom filter false positive rate | No |
| `diskIoBufferSize` | Disk I/O buffer size | Yes |
| `contextLoggingEnabled` | MDC-based context logging | Yes |

## Custom data types

If you introduce a custom key or value type, implement
`com.hestiastore.index.datatype.TypeDescriptor` and wire it through the
builder. See [Data Types](data-types.md) for the contract details.
