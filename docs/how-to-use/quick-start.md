# Quick Start

This page gets a minimal index running first, then shows the next practical
steps: persistence, reopening, iteration, and maintenance.

## In-memory example

```java
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;

public class Example {
  public static void main(String[] args) {
    Directory directory = new MemDirectory();

    IndexConfiguration<String, String> conf = IndexConfiguration
        .<String, String>builder()
        .identity(identity -> identity
            .name("example")
            .keyClass(String.class)
            .valueClass(String.class))
        .build();

    try (SegmentIndex<String, String> index = SegmentIndex.create(directory, conf)) {
      index.put("hello", "world");
      System.out.println(index.get("hello"));
    }
  }
}
```

## Persist data on the filesystem

```java
import java.io.File;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FsDirectory;

Directory directory = new FsDirectory(new File("/var/lib/hestiastore/orders"));
```

When a filesystem-backed index is open, HestiaStore uses a `.lock` file to
prevent multiple writers from opening the same directory unsafely.

## Open an existing index

```java
SegmentIndex<String, String> index = SegmentIndex.open(directory, conf);
```

Use `create(...)` for a new index and `open(...)` for an existing one.

## Basic operations

```java
index.put("hello", "world");
String value = index.get("hello");
index.delete("hello");
```

Insert only when a key is logically absent:

```java
boolean inserted = index.putIfAbsent("order-100", "created");
```

Replace only when the current value matches the expected value:

```java
boolean replaced = index.replace("order-100", "created", "paid");
```

`putIfAbsent(...)` and `replace(...)` are currently supported only when WAL is
disabled. They throw `IndexException` when WAL is enabled.

## Store emoji and multilingual text

HestiaStore supports strict UTF-8 keys and values through explicit string
descriptors. Select them when creating the index:

```java
import org.hestiastore.index.datatype.TypeDescriptorTinyUtf8String;
import org.hestiastore.index.datatype.TypeDescriptorUtf8String;

IndexConfiguration<String, String> conf = IndexConfiguration
    .<String, String>builder()
    .identity(identity -> identity
        .name("localized-text")
        .keyClass(String.class)
        .valueClass(String.class)
        .keyTypeDescriptor(new TypeDescriptorTinyUtf8String())
        .valueTypeDescriptor(new TypeDescriptorUtf8String()))
    .build();

try (SegmentIndex<String, String> index = SegmentIndex.create(directory, conf)) {
    index.put("objednávka-🙂", "Čeština Ελληνικά Кириллица 日本語 👨‍👩‍👧‍👦");
}
```

Use `TypeDescriptorTinyUtf8String` for payloads up to 255 encoded bytes and
`TypeDescriptorUtf8String` for larger strings. The limits apply to UTF-8 bytes,
not displayed characters.

The default `String` descriptor remains ISO-8859-1 for on-disk compatibility.
Choose the UTF-8 descriptors before creating an index that will contain emoji
or characters outside ISO-8859-1. See [Data Types](../configuration/data-types.md#utf-8-string-descriptors)
for exact size limits and migration guidance.

## Iterate entries

Read all entries in ascending key order:

```java
index.getStream().forEach(entry -> System.out.println(entry));
```

Read a half-open key range in ascending key order:

```java
try (var stream = index.scan("order-100", "order-200")) {
    stream.forEach(entry -> System.out.println(entry));
}
```

The lower bound is inclusive and the upper bound is exclusive. Pass `null` as
the upper bound to scan from the lower key through the end of the index.

Read only selected segments:

```java
import org.hestiastore.index.segmentindex.SegmentWindow;

SegmentWindow window = SegmentWindow.of(1000, 10);
index.getStream(window).forEach(entry -> System.out.println(entry));
```

## Maintenance operations

- `flush()` persists in-memory changes.
- `checkAndRepairConsistency()` validates and repairs recoverable issues.
- `compact()` rewrites fragmented data into a cleaner layout.

```java
index.maintenance().flush();
index.maintenance().checkAndRepairConsistency();
index.maintenance().compact();
```

## Practical limits to know early

- Call `flush()` before streaming if recent writes must be included.
- Avoid mutating the same index while consuming a stream when you need a stable
  view.
- Expect synchronization overhead under heavy contention.

## Next steps

- [Configuration](../configuration/index.md) for storage and tuning knobs
- [Data Types](../configuration/data-types.md) for UTF-8 and custom descriptors
- [WAL](../operations/wal.md) for local crash recovery
- [Troubleshooting](troubleshooting.md) for common startup and runtime issues
