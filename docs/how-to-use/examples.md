# 📚 Examples of HestiaStore Usage

This page shows common usage patterns with correct imports and short explanations.

## 👋 Hello World (In‑Memory)

Simple example that creates in‑memory storage, then opens an index in this storage, writes one key‑value pair, and closes the index.

```java
import org.hestiastore.index.sst.Index;
import org.hestiastore.index.sst.IndexConfiguration;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;

public class Example {
  public static void main(String[] args) {
    // Create an in‑memory directory implementation
    Directory directory = new MemDirectory();

    // Prepare index configuration
    IndexConfiguration<String, String> conf = IndexConfiguration
        .<String, String>builder()
        .withKeyClass(String.class)
        .withValueClass(String.class)
        .withName("test_index")
        .build();

    // Index is AutoCloseable — prefer try‑with‑resources
    try (Index<String, String> index = Index.create(directory, conf)) {
      index.put("Hello", "World");
      String value = index.get("Hello");
      System.out.println("Value for Hello: " + value);
    }
  }
}
```

Once this works, explore the advanced [configuration](../../configuration) for directory types and custom key/value classes.

## 💾 Filesystem Usage

Storing data to the file system is the main function of the library. A file system directory can be used like this:

```java
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FsDirectory;
import java.io.File;

// Create a file system directory
Directory directory = new FsDirectory(new File("../some/directory/"));
```

This immediately creates the initial index files and makes it ready to use.

> Note: When an index works with a directory, it locks it with a `.lock` file. When the index is closed, the lock is removed.

## 📂 Opening an Existing Index

Use a dedicated open method for existing indexes:

```java
import org.hestiastore.index.sst.Index;
import org.hestiastore.index.sst.IndexConfiguration;

IndexConfiguration<String, String> conf = IndexConfiguration
    .<String, String>builder()
    .withKeyClass(String.class)
    .withValueClass(String.class)
    .withName("test_index")
    .build();

Index<String, String> index = Index.open(directory, conf);
```

## ✍️ Data Manipulation

Put and get are straightforward:

```java
index.put("Hello", "World");
String value = index.get("Hello");
```

Stored values are immediately available.

## 📈 Sequential Data Reading

Read all entries in ascending key order:

```java
index.getStream().forEach(entry -> {
  System.out.println("Entry: " + entry);
});
```

Select a subset of entries by segment window (offset, limit):

```java
import org.hestiastore.index.sst.SegmentWindow;

// Only data from selected segments will be returned
SegmentWindow window = SegmentWindow.of(1000, 10);
index.getStream(window).forEach(entry -> System.out.println(entry));
```

## 🧹 Data Maintenance

Maintenance operations available on Index:

- `flush()` Flushes in‑memory data to disk. Useful before iterating or when you need to ensure data is durable.
- `checkAndRepairConsistency()` Verifies metadata and segment consistency and attempts repairs; fails if beyond repair.
- `compact()` Compacts segments and can reduce disk usage.

```java
// After flush, all data changes are persisted. It is similar to a transaction commit.
index.flush();

// Verify consistency or try to repair
index.checkAndRepairConsistency();

// Data may be fragmented; this recalculates all segments
index.compact();
```

## ⚠️ Limitations

### 🌀 Streaming Consistency

Streaming uses a snapshot at iteration time and does not use the index cache to avoid mid‑iteration mutations breaking iteration. To reduce stale results:

- Call `flush()` before streaming if recent writes must be included.
- Avoid calling `put()` or `delete()` while iterating a stream.
- For full snapshot reads, consider `compact()` beforehand to simplify segments.

### 🔒 Thread Safety

Index is not thread‑safe by default. Enable synchronization via configuration. See option [withThreadSafe](../../configuration#thread-safe-withthreadsafe).

## 🧨 Exception Handling

Common runtime exceptions you may encounter:

- `NullPointerException` – Severe I/O or unexpected nulls (e.g., corrupted files).
- `IndexException` – Internal consistency issues detected by the index.
- `IllegalArgumentException` – Validation errors (e.g., missing key type).
- `IllegalStateException` – Inconsistent state preventing recovery.

All exceptions are runtime exceptions and do not need explicit catching.
