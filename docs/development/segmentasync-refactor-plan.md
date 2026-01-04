# SegmentAsync Refactor Plan

This document tracks the phased refactor of the segment async layer. Keep it
updated as changes land.

## How To Update
- Mark the current phase as `IN_PROGRESS`.
- When a phase is finished, mark it `DONE` and include a short note.
- Keep next steps small and explicit.

## Phases
1) Phase 1 - Extract maintenance scheduling into a dedicated component.
   Status: DONE
   Notes: SegmentMaintenanceScheduler owns policy + enqueue logic.

2) Phase 2 - Introduce MaintenanceState to coordinate flush/compact/split.
   Status: DONE
   Notes: Added maintenance task/state tracking in scheduler + adapter.

3) Phase 3 - Add SegmentAsyncSplitCoordinator for async splitting.
   Status: DONE
   Notes: Async split scheduling added with in-flight de-duplication.

4) Phase 4 - Expand tests for async maintenance + split interplay.
   Status: DONE
   Notes: Added split-vs-flush serialization test + async split wait in put test.

5) Phase 5 - Simplify SegmentIndexImpl to delegate maintenance decisions.
   Status: DONE
   Notes: Post-write maintenance moved to SegmentMaintenanceCoordinator.
