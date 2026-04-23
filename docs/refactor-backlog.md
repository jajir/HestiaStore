# Refactor backlog

## Open Items

### Strategic epics

### To Solve

[ ] 80. Make WAL durability explicit for non-fsync storage adapters so `SYNC` and `GROUP_SYNC` never claim guarantees that the active storage backend cannot provide.
[ ] 81. Split `WalRuntime` into focused writer, recovery, segment-catalog, and sync-policy responsibilities instead of keeping all lifecycle paths behind one shared monitor.
[ ] 82. Replace the slow generic `WalStorageDirectory` fallback with an explicit seekable/capability-aware storage path, or reject unsupported backends early.

### Regular Maintenance

[ ] M37 Audit `segment` package for unused or test-only code (Risk: LOW)
[ ] M38 Review `segment` package for test and Javadoc coverage (Risk: LOW)
[ ] M39 Audit `segmentindex` package for unused or test-only code (Risk: LOW)
[ ] M40 Review `segmentindex` package for test and Javadoc coverage (Risk: LOW)
[ ] M41 Audit `segmentregistry` package for unused or test-only code (Risk: LOW)
[ ] M42 Review `segmentregistry` package for test and Javadoc coverage (Risk: LOW)

## Done (Archive)
