# Quick Start

This page gets a minimal index running first, then shows the next practical
steps: persistence, reopening, iteration, and maintenance.

## In-memory example

```java
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
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

## Iterate entries

Read all entries in ascending key order:

```java
index.getStream().forEach(entry -> System.out.println(entry));
```

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
index.flush();
index.checkAndRepairConsistency();
index.compact();
```

## Practical limits to know early

- Call `flush()` before streaming if recent writes must be included.
- Avoid mutating the same index while consuming a stream when you need a stable
  view.
- Expect synchronization overhead under heavy contention.

## Next steps

- [Configuration](../configuration/index.md) for storage and tuning knobs
- [WAL](../operations/wal.md) for local crash recovery
- [Troubleshooting](troubleshooting.md) for common startup and runtime issues
