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

[x] 83. Define the new split runtime contract around `hintSplitCandidate(...)`, `awaitQuiescence(...)`, and managed lifecycle shutdown, and remove public scheduling concepts such as full-scan requests from the intended service shape.
[x] 84. Introduce a managed split runtime skeleton with explicit `OPENING -> RUNNING -> CLOSING -> CLOSED` state transitions and fail-fast behavior for calls made outside `RUNNING`.
[x] 85. Replace the current split-policy work-state loop with a candidate registry built from `Map<SegmentId, State>` plus a blocking ready queue so split hints are deduplicated and workers block instead of polling.
[x] 86. Split policy evaluation from split execution so policy workers only validate mapping and threshold eligibility, then hand off accepted candidates to the dedicated split executor.
[x] 87. Rebuild periodic reconciliation around the new candidate registry so the 250 ms scanner only offers over-threshold mapped segments that are not already queued or in process.
[x] 88. Rework split runtime tests around lifecycle, deduplicated candidate scheduling, blocking worker wakeup, quiescence, and close-drain behavior before removing the remaining legacy split-policy orchestration.
