# 🛠️ Troubleshooting

Common issues and quick fixes.

## 🔒 ".lock" File Prevents Opening

- Cause: Another process holds the index open, or a previous run did not close cleanly.
- Fix: Ensure the process using the index has terminated and closed the index. If safe, remove the stale .lock file after verifying no process uses the directory.

## 🧩 Consistency Errors (IndexException)

- Symptom: Consistency checks fail or reads behave unexpectedly after a crash.
- Fix: Run checkAndRepairConsistency(). If it still fails, restore from a backup.

## ⚠️ Exception 'Attempt to insert the same key as previous'

When followin exception appears in logs. Than it's probably problem with inconsistent implementaion of custom data type. Please look at [custom type configuration](../configuration/data-types.md#custom-data-types)

```text
xception in thread "main" java.lang.IllegalArgumentException: Attempt to insert the same key as previous. Key(Base64)='QUFHR0JHQkFBVERTU1BTUw==', comparator='DtFixedLengthByteArray$$Lambda/0x0000000800214238'
    at org.hestiastore.index.sorteddatafile.SortedDataFileWriter.verifyKeyOrder(SortedDataFileWriter.java:76)
    at org.hestiastore.index.sorteddatafile.SortedDataFileWriter.write(SortedDataFileWriter.java:104)
    at org.hestiastore.index.GuardedEntryWriter.write(GuardedEntryWriter.java:18)
    at org.hestiastore.index.segment.SegmentDeltaCacheWriter.lambda$doClose$0(SegmentDeltaCacheWriter.java:87)
    at org.hestiastore.index.WriteTransaction.execute(WriteTransaction.java:41)
    at org.hestiastore.index.segment.SegmentDeltaCacheWriter.doClose(SegmentDeltaCacheWriter.java:84)
    at org.hestiastore.index.AbstractCloseableResource.close(AbstractCloseableResource.java:23)
    at org.hestiastore.index.segment.SegmentDeltaCacheCompactingWriter.doClose(SegmentDeltaCacheCompactingWriter.java:47)
    at org.hestiastore.index.AbstractCloseableResource.close(AbstractCloseableResource.java:23)
    at org.hestiastore.index.segmentindex.CompactSupport.flushToCurrentSegment(CompactSupport.java:84)
    at org.hestiastore.index.segmentindex.CompactSupport.compact(CompactSupport.java:59)
    at java.base/java.util.ArrayList.forEach(ArrayList.java:1596)
    at org.hestiastore.index.segmentindex.SegmentIndexImpl.flushCache(SegmentIndexImpl.java:126)
    at org.hestiastore.index.segmentindex.SegmentIndexImpl.put(SegmentIndexImpl.java:83)
    at org.hestiastore.index.segmentindex.SegmentIndex.put(SegmentIndex.java:93)
```

## 📦 Dependency Resolution Fails

- Maven: run the command below and confirm org.hestiastore.index:core is present.

    mvn dependency:tree

- Gradle: run the command below for dependency insight.

    ./gradlew dependencyInsight --dependency org.hestiastore.index:core

- Also verify you used the latest version from Maven Central.

## ☕ Java Version Mismatch

- Ensure Java 11+ is used (Java 17 recommended). Check with java -version and align your IDE or CI JDK.

## 📁 Permission or Path Errors

- Ensure your process has read/write permissions to the target directory.
- Use absolute paths for clarity in services or containers.

## 🔄 Stale Streaming Results

- Call flush() before streaming if you require latest writes.
- Avoid put() or delete() while iterating a stream.

## ❓ Need More Help?

- Search existing tickets: <https://github.com/jajir/HestiaStore/issues?q=is%3Aissue>
- Open a new ticket: <https://github.com/jajir/HestiaStore/issues/new/choose>

When opening a ticket, please include:

- Your Java version (run `java -version`)
- HestiaStore version and build tool (Maven/Gradle)
- Minimal code snippet or steps to reproduce
- Relevant logs/stack traces and OS info
