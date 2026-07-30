# Release 1.1.0

HestiaStore 1.1.0 is the first feature release after the stable 1.0.0
publication. The release strengthens the public API for embedded Java
applications that need local persistence, ordered keys, bounded-memory lookups,
and operationally simple storage without adding a separate database service.

## Highlights

- Bounded key-range scans are now available through
  `SegmentIndex.scan(fromInclusive, toExclusive)` and
  `SegmentIndex.scan(fromInclusive, toExclusive, isolation)`.
- Conditional write operations are now part of `SegmentIndex`:
  `putIfAbsent(key, value)` and
  `replace(key, expectedValue, newValue)`.
- Strict UTF-8 string descriptors support emoji and multilingual keys and
  values without changing the default on-disk `String` compatibility mode.
- Composite-key examples show how to model ordered multi-part keys with custom
  descriptors.
- Benchmark coverage now includes bounded range scans, persisted mutations, hot
  route writes, and branch-based per-change benchmark history.
- Internal changes improve point-operation synchronization, route leasing, WAL
  append concurrency, cache memory retention, and sorted cache snapshot
  allocation.

## Dependency

```xml
<dependencies>
  <dependency>
    <groupId>org.hestiastore</groupId>
    <artifactId>engine</artifactId>
    <version>1.1.0</version>
  </dependency>
</dependencies>
```

Gradle:

```kotlin
dependencies {
  implementation("org.hestiastore:engine:1.1.0")
}
```

## API Changes

`SegmentIndex` now declares two new conditional mutation methods:

```java
boolean inserted = index.putIfAbsent(key, value);
boolean replaced = index.replace(key, expectedValue, newValue);
```

These methods are currently supported only when WAL is disabled. They throw
`IndexException` when WAL is enabled.

Range scans use half-open bounds:

```java
try (var stream = index.scan(fromInclusive, toExclusive)) {
    stream.forEach(entry -> System.out.println(entry));
}
```

The lower bound is inclusive, the upper bound is exclusive, and a `null` upper
bound scans through the end of the index.

## Breaking Changes

- External implementations of `SegmentIndex` must implement
  `putIfAbsent(...)` and `replace(...)`.
- `SegmentIndex.scan(...)` is now part of the public API. Implementations that
  do not support bounded scans may keep the default unsupported behavior, but
  callers can now compile against the range-scan contract.

## Compatibility Notes

- The default `String` descriptor remains ISO-8859-1 for on-disk compatibility.
  Use `TypeDescriptorTinyUtf8String` or `TypeDescriptorUtf8String` explicitly
  before creating indexes that must store emoji or multilingual text.
- `TypeDescriptorTinyUtf8String` stores payloads up to 255 encoded bytes.
  `TypeDescriptorUtf8String` supports larger payloads with canonical unsigned
  LEB128 length framing.
- `SegmentIteratorIsolation.FAIL_FAST` can stop a scan early if maintenance,
  segment eviction, unloading, or index closing invalidates the iterator. Use
  result counts accordingly; do not treat a fail-fast count as proof that a
  complete range was visited.

## Operational Artifacts

The release build also produces the standalone operational tool distributions:

- `index-tools-1.1.0.zip` plus `index-tools-1.1.0.zip.sha256`
- `wal-tools-1.1.0.zip` plus `wal-tools-1.1.0.zip.sha256`

Publish this GitHub release against the `release-1.1.0` tag after the Maven
Central deployment succeeds and the tag has been pushed.
