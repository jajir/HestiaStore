---
title: Bulk Ingestion with Senku
audience: user
doc_type: tutorial
owner: engine
---

# Bulk Ingestion with Senku

Use Senku to ingest a finite dataset, merge duplicate keys, finalize the result,
and scan it in key order. Multiple producer threads can share a writing handle.
After finalization, the index is immutable and supports one active sorted stream
at a time.

Senku is part of the `engine` module, in `org.hestiastore.index.senku`. Use
[SegmentIndex](quick-start.md) when you need point lookups, further mutations,
bounded range scans, or WAL recovery. Senku has no point lookup, delete, WAL, or
resume API.

## Create, Write, and Finalize

This complete example uses an in-memory directory and keeps the maximum value
for each key. The settings are small demonstration values; size them for your
workload before loading a large dataset.

```java
import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.senku.SenkuIndex;
import org.hestiastore.index.senku.SenkuMergeFunctionRegistry;
import org.hestiastore.index.senku.SenkuReady;
import org.hestiastore.index.senku.SenkuWriting;

public final class SenkuExample {
    public static void main(String[] args) {
        Directory directory = new MemDirectory();
        TypeDescriptorLong longs = new TypeDescriptorLong();
        SenkuMergeFunctionRegistry<Long, Long> functions =
                new SenkuMergeFunctionRegistry<>();
        functions.register((key, first, second) -> Math.max(first, second));

        SenkuWriting<Long, Long> writing = SenkuIndex
                .builder(directory, longs, longs, functions)
                .shardHashFunction(key -> Long.hashCode(key))
                .shardCount(4)
                .maxInMemoryEntries(1_024)
                .maxKeysPerPage(256)
                .mergeFanIn(4)
                .maintenanceThreads(2)
                .diskIoBufferSize(8_192)
                .create();

        writing.put(7L, 10L);
        writing.put(2L, 5L);
        writing.put(7L, 20L);

        try (SenkuReady<Long, Long> ready = writing.finishWriting()) {
            System.out.println("Records: " + ready.recordCount());
            try (Stream<Entry<Long, Long>> entries = ready.openStream()) {
                entries.forEach(entry -> System.out.println(
                        entry.getKey() + "=" + entry.getValue()));
            }
        }
    }
}
```

Expected output:

```text
Records: 2
2=5
7=20
```

`finishWriting()` stops admission, flushes buffered entries, and waits for all
required merges and final metadata publication. It can take substantial time
and temporary disk space. The returned ready handle inherits the directory
lock; the old writing handle rejects further writes and finalization calls.
An empty input can also be finalized and produces a zero-record ready index.

Join all producer tasks before calling `finishWriting()` when every intended
write must be included. A write racing with finalization can be rejected, and a
losing concurrent finish call does not join the winning call.

## Own the Handles and Streams

1. Every healthy writing handle must reach `finishWriting()`. `SenkuWriting`
   has no `close()` or abort operation. Dropping it leaves owned non-daemon
   maintenance threads and the directory lock active, so validate application
   inputs and arrange producer completion before creating the handle.
2. Close the returned `SenkuReady` with try-with-resources. It owns the
   directory lock and closes an active stream during handle close. Repeated
   successful ready close is harmless.
3. Close each stream explicitly, even after full consumption, a short-circuit
   operation such as `findFirst()`, or a downstream exception. Exhaustion alone
   does not free the active-stream slot. After stream close, the same ready
   handle can open another stream.
4. Consume each stream sequentially. Coordinate consumption with stream and
   handle close; advancing a cursor concurrently with close is unsupported.
5. An operational writing failure invalidates that build. Background shutdown
   stops owned work and releases the lock, but does not delete build artifacts.
   Failed or unfinished builds cannot be resumed. Dispose of their directory
   after its owner has stopped; do not delete or alter live index files.

A ready-stream failure is reported at the failing call and does not establish a
retained ready-handle error state. Close the stream and handle when a scan
fails. Reopening a stream does not repair storage.

## Choose a Merge Function and Key Identity

Register exactly one merge function. The registry freezes after successful
creation. The function can run on producer and maintenance threads, and Senku
does not preserve input order or reduction grouping. It must be:

- deterministic, associative, commutative, and thread-safe
- fast and non-blocking, without I/O or dependencies on application lock order
- non-null in its result; returning null is a failure, not deletion

Keys, values, and merge results must be non-null and must not be mutated after
submission. A value matching a descriptor's tombstone is an ordinary Senku
value; Senku does not interpret deletion markers.

For generic keys, comparison, equality, and hashing must describe the same
identity: `compare(a, b) == 0` exactly when `a.equals(b)`, and equal keys must
have equal `hashCode()` values and the same configured shard hash. The configured
hash must be stable and thread-safe. Merely mapping equal keys to the same final
shard is insufficient because Senku also uses the hash to choose a mutation
stripe and probe its table. Violating these requirements can lose duplicate
reduction or sorted-order guarantees.

## Use Primitive Long Sets

For exact membership deduplication of signed long keys, explicitly select the
built-in descriptors and `SenkuMergeFunctions.longSet()` reducer. This enables
`putLong` and the bounded batch API without boxed input keys:

```java
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorNull;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.senku.SenkuIndex;
import org.hestiastore.index.senku.SenkuLongSetWriting;
import org.hestiastore.index.senku.SenkuMergeFunctionRegistry;
import org.hestiastore.index.senku.SenkuMergeFunctions;
import org.hestiastore.index.senku.SenkuReady;

SenkuMergeFunctionRegistry<Long, NullValue> functions =
        new SenkuMergeFunctionRegistry<>();
functions.register(SenkuMergeFunctions.longSet());

SenkuLongSetWriting writing = SenkuIndex
        .builder(new MemDirectory(), new TypeDescriptorLong(),
                new TypeDescriptorNull(), functions)
        .shardCount(4)
        .maxInMemoryEntries(1_024)
        .maxKeysPerPage(256)
        .mergeFanIn(4)
        .maintenanceThreads(2)
        .createLongSet(Long::hashCode);

writing.putLong(Long.MIN_VALUE);
long[] keys = {0L, 7L, 7L, Long.MAX_VALUE};
writing.putLongs(keys, 0, keys.length);
try (SenkuReady<Long, NullValue> ready = writing.finishWriting()) {
    System.out.println(ready.recordCount()); // 4
}
```

`createLongSet(hash)` supplies its own primitive hash, replacing a generic hash
set on the builder. Custom descriptor subclasses or an equivalent user callback
do not select this specialized API.

`putLongs` validates the whole slice before accepting keys and does not retain
or modify the array. The caller must leave the selected slice unchanged until
the call returns. The batch is not a transaction: interruption, an operational
failure, or concurrent finalization can leave an accepted subset, which need
not be an input prefix. A valid empty slice is a no-op only while writing.

## Persist and Reopen

To use filesystem storage, replace the example's directory with an empty
application-owned directory:

```java
Directory directory = new FsDirectory(new File("/var/lib/hestiastore/bulk"));
```

Import `java.io.File` and `org.hestiastore.index.directory.FsDirectory`.
After successful finalization and ready close, reopen it with the same key and
value descriptors, comparator semantics, and I/O buffer size:

```java
try (SenkuReady<Long, Long> ready = SenkuIndex.open(
        directory, new TypeDescriptorLong(), new TypeDescriptorLong(), 8_192);
        Stream<Entry<Long, Long>> entries = ready.openStream()) {
    entries.forEach(entry -> System.out.println(entry.getKey()));
}
```

The shard count, key-page codec, and compression configuration are stored in
metadata. Type/comparator identity and `diskIoBufferSize` are not stored or
compared when reopening; byte-compatible mismatches can silently produce
incorrect results. Keep those settings with your application's dataset
configuration. The merge registry and write-path tuning are unnecessary for
reopening.

`ready.properties` is published last and marks a logically finalized index.
Senku adds no `fsync`, atomic-rename, or power-loss guarantee beyond the
`Directory` backend, and performs no recovery of unfinished builds. Its lock
also depends on that backend: the supplied filesystem lock uses a lock-file
check followed by creation, so it does not establish atomic exclusion between
racing processes. Arrange one owner for the directory at the application level.

## Tune and Inspect the Result

- `maxInMemoryEntries` requests batch rotation using an approximate distinct-key
  count. One detached batch can be flushing while another fills; it is not a
  heap-byte limit.
- `maxKeysPerPage`, `mergeFanIn`, and `maintenanceThreads` jointly determine
  active merge memory. Larger settings can increase retained pages and buffers.
- `maintenanceQueueSize` bounds queued jobs. Backpressure does not reserve disk
  space or bound the total committed merge backlog.
- The default key codec is `KeyPageCodecs.prefix()` and compression is
  `Compression.zstd(3)`. Zstd uses the bundled `zstd-jni` native runtime. For
  natural long keys, `KeyPageCodecs.longDeltaVarint()` is an explicit option;
  fixed-weight codecs additionally require every key to satisfy their configured
  domain. Codecs and compression are selected on the writing builder.
- `recordCount()` returns the exact committed metadata count without scanning
  pages. `longKeySummary()` optionally returns at most 4,096 weighted natural-long
  representatives; weights sum to the count, but the distribution is approximate
  and must not be used for membership. Neither method is a data-integrity audit.

## Related Documentation

- [Senku Architecture and Contracts](../architecture/senku-index.md), including
  the [complete builder reference](../architecture/senku-index.md#builder-configuration-summary).
- [Data Types](../configuration/data-types.md) for descriptor implementations.
- [Benchmark Guide](https://github.com/jajir/HestiaStore/blob/main/benchmarks/README.md) for Senku workload profiles.
