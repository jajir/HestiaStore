# Metrics Snapshot

`SegmentIndex.metricsSnapshot()` exposes a stable, read-only metrics contract
for index-level telemetry.

## Current fields

- `getOperationCount`  
  Total number of `get(...)` calls accepted by the index instance.
- `putOperationCount`  
  Total number of `put(...)` calls accepted by the index instance.
- `deleteOperationCount`  
  Total number of `delete(...)` calls accepted by the index instance.
- `state`  
  Current `SegmentIndexState` at snapshot creation time.

## Semantics

- Snapshot is immutable.
- Counts are monotonic for one index instance lifetime.
- Counters are process-local and reset when a new index object is created.
- Field values represent observed operation calls, not necessarily durable
  writes on disk.

## Compatibility policy

- Existing fields and meaning are stable and cannot change silently.
- New fields may be added in future versions.
- Consumers should ignore unknown fields for forward compatibility.
