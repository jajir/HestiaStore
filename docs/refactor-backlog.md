# Refactoring backlog

## Active (what we want to solve acctualy)

[ ] - from segment index don't call flush, it could decide only user or segment.
[ ] - segment interface should have wrapper `SegmentSyncAdapters` that synchronize segment interface. When segment operation replay BUSY that it wait few milicesonds and than repeat until OK and result is returned or ERROR or CLOSED than it throw exception. 
[ ] - Maximal wainting time for BUSY will have timeput defined in miliseconds or seconds, because there is a waint for split operation.
[ ] - segment switching after `flush()` and `compact()` should not relay on file rename. Point whole index to new version.
[ ] - consider segment per directory
[ ] - segmentindex: remove global cpu executor serialization; execute sync calls on caller threads.
[ ] - segmentindex: use shared, bounded maintenance executor (default 10 threads) configurable via withNumberOrSegmentMaintenanceThreads. This executor shoud be just for segment maintenance.
[ ] - segmentindex: run flush/compact/split on the shared maintenance executor only (no per-segment executors).
[ ] - segmentindex: use shared, bounded maintenance executor (default 10 threads) configurable via withNumberOrSegmentIndexMaintenanceThreads. This executor shoud be just for segmentindex maintenance (split(), ..).
[ ] - segmentindex: add internal SegmentIndexCore with IndexResult (OK/BUSY/ERROR/CLOSED) for coordination; keep public API BUSY-free.

## Ready
- (move items here when they are scoped and ready to execute)

## In Progress
- (move items here when actively working)

## Done (Archive)
- (keep completed items here; do not delete)
