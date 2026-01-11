# Refactor backlog

## Active (segmentindex refactor plan - class level)

### Split executor isolation + config

[ ] Add new configuration property `numberOfIndexMaintenanceThreads` with
    default `10` in `IndexConfigurationContract`, thread it through
    `IndexConfiguration`, `IndexConfigurationBuilder`, and
    `IndexConfigurationManager` (keep existing
    `numberOfSegmentIndexMaintenanceThreads` for non-split maintenance).
[ ] Extend `IndexConfiguratonStorage` to persist the new property (read/write)
    and keep backward compatibility with configs that do not define it.
[ ] Introduce a dedicated split executor (separate thread group) in
    `SegmentRegistry` or a new `SegmentSplitExecutor` wrapper; size it from
    `numberOfIndexMaintenanceThreads` and manage lifecycle on close.
[ ] Update `SegmentAsyncSplitCoordinator` to use the new split executor and
    add a "start latch" so callers can wait until the split task actually
    begins execution (not just queued).
[ ] Change `SegmentMaintenanceCoordinator.handlePostWrite` to block only
    until the split task starts, then return to the caller; remove `.join()`
    on split completion. Use `indexBusyTimeoutMillis` to cap waiting.
[ ] Add tests for:
    - split task starts without blocking `put` until completion
    - timeout path when the split executor is saturated
    - concurrency with flush/compact (regression for timeout)
[ ] Update configuration docs to describe the new property and defaults.

## Ready

- (move items here when they are scoped and ready to execute)

## Deferred (segment scope, do not touch now)

[ ] - segment: from segment index do not call flush; only user or segment decides.
[ ] - segment: add SegmentSyncAdapters wrapper to retry BUSY with backoff until OK or throw on ERROR/CLOSED.
[ ] - segment: add configurable BUSY timeout to avoid infinite wait (split waits).
[ ] - segment: avoid file rename for flush/compact switching; point index to new version.
[ ] - segment: consider segment per directory.

## In Progress

- (move items here when actively working)

## Done (Archive)

- (keep completed items here; do not delete)
