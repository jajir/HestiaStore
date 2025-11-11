# 🏭 Production Guide

Operational guidance for running HestiaStore in production.

- Deployment considerations and sizing
- File system and storage layout
- Backup and restore strategy
- Observability and logging

## ⚠️ After Unexpected Shutdown

Recommended steps to verify integrity and reclaim optimal layout:

- Reopen the index and run a consistency check to validate segments and the key→segment map.
- Optionally run a compaction to merge delta caches into main SST files and rebuild auxiliary structures (sparse index, Bloom filter).
- Take a fresh backup after compaction completes.

Example (Java):

```java
// Open existing index (types omitted for brevity)
Index<Integer, String> index = Index.open(directory);

// 1) Verify internal consistency (throws IndexException on irreparable issues)
index.checkAndRepairConsistency();

// 2) Optionally compact to fold delta caches into main SST files
index.compact();

// 3) Create a new backup snapshot
//   (use your backup process; see operations/backup-restore.md)

index.close();
```

Notes:
- HestiaStore has no WAL; durability comes from flush/close boundaries and atomic file replacement on commit.
- If you keep running without compaction, reads remain correct; compaction improves locality and space usage.
