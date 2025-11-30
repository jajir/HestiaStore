# 🔒 Access Model

HestiaStore offers two usage modes: a fast, non‑synchronized default intended for single‑threaded (or externally synchronized) use, and an opt‑in thread‑safe variant that serializes operations with a coarse lock. Iteration is protected with an optimistic lock so scans don’t observe torn updates.

## 🧬 Variants

- Default (non‑synchronized): `segmentindex/SegmentIndexImpl`
  - Highest throughput, minimal coordination overhead.
  - Not thread‑safe: internal structures (maps, caches) are not synchronized.
  - Use from a single thread, or add your own external synchronization.

- Thread‑safe: `segmentindex/IndexInternalSynchronized`
  - Enabled via `IndexConfigurationBuilder.withThreadSafe(true)`.
  - Wraps all public operations (`put`, `get`, `delete`, `flush`, `compact`, `getStream`, `checkAndRepairConsistency`) in a single `ReentrantLock`.
  - Iterators returned by `getStream` are wrapped with `EntryIteratorSynchronized` to take/release the lock for `hasNext`/`next`/`close` calls.
  - Trade‑off: simple and safe, but long scans will contend with writers due to coarse locking.

Code pointers: `segmentindex/IndexInternalSynchronized.java`, `segmentindex/EntryIteratorSynchronized.java`.

## 🔐 Process Exclusivity

Opening an index acquires a directory file lock to prevent two processes from using the same directory at once:

- On open: `IndexStateNew` creates `.lock` via `Directory.getLock()` and transitions to `IndexStateReady`.
- On close: the lock file is removed in `IndexStateReady#onClose`.
- Any operation before “ready” or after “closed” throws an error (`IndexState*`).

Code pointers: `segmentindex/IndexStateNew.java`, `segmentindex/IndexStateReady.java`, `segmentindex/IndexStateClose.java`, `directory/FsFileLock.java`.

## 🧪 Reader Isolation (Optimistic Lock)

Segment reads are protected by an optimistic lock based on a monotonically increasing version:

- Each segment has a `VersionController` implementing `OptimisticLockObjectVersionProvider`.
- Writers bump the version before delta writes and compaction.
- Per‑segment iterators are wrapped with `EntryIteratorWithLock` holding a snapshot of the version. If the version changes mid‑scan, `hasNext()` returns false and `next()` throws, avoiding torn reads.

Code pointers: `segment/VersionController.java`, `EntryIteratorWithLock.java`, `segment/SegmentImpl#openIterator()`.

## ✍️ Writers and Consistency

- Delta cache writes: `SegmentDeltaCacheCompactingWriter` opens a per‑segment writer, collects updates, and may trigger compaction when policy advises. Writers close before compaction; compaction runs under a fresh version.
- Full compaction: `SegmentCompacter#forceCompact` rewrites the segment via `SegmentFullWriterTx` (transactional), then clears delta and updates properties.
- Atomicity across files is guaranteed by the write‑transaction pattern (`open()` → close → `commit()`; `*.tmp` + atomic rename). See “Recovery”.

Code pointers: `segment/SegmentDeltaCacheCompactingWriter.java`, `segment/SegmentCompacter.java`, `segment/SegmentFullWriter*.java`.

## 🚦 Contention Hotspots and Mitigation

- Thread‑safe variant’s single lock:
  - Hotspot when mixing long `getStream()` scans with frequent writes. Consider scanning with bounded `SegmentWindow` or running scans off‑peak.
  - Keep individual operations short; avoid long‑held locks in user callbacks.

- Iteration under mutation:
  - Optimistic lock will terminate iterators if a segment mutates (e.g., compaction). Re‑open the stream if needed.

- Flush/compaction:
  - These operations modify many files and bump versions; plan to run them during low traffic if using the synchronized variant.

## ⚙️ Configuration Tips

- Enable thread‑safe mode when you need concurrent access without external coordination:

```java
IndexConfiguration<Integer, String> conf = IndexConfiguration.<Integer, String>builder()
    // ... type descriptors and other settings ...
    .withThreadSafe(true)
    .build();
SegmentIndex<Integer, String> index = SegmentIndex.create(directory, conf);
```

- For high read concurrency with minimal contention, prefer the default variant and place your own read/write locks at a higher level if needed.

## 🔗 Related Glossary

- [Segment](glossary.md#segment)
- [Write Transaction](glossary.md#write-transaction)
- [SegmentWindow](glossary.md#segmentwindow)
- [Compaction](glossary.md#compaction)
- [Consistency Checker](glossary.md#consistency-checker)
