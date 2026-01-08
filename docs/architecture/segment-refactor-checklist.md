# Segment Refactor Checklist (Threading in segmentindex)

## Goals
- SegmentCore is single-threaded and only executes commands; it does not manage
  state, threads, or retries.
- SegmentImpl owns SegmentStateMachine and an executor provided by higher level
  code; it performs state checks and delegates to SegmentCore.
- Threading, scheduling, futures, and retry logic live in segmentindex, not in
  segment.

## Assumptions to confirm
- SegmentImpl is the only concrete Segment implementation used by segmentindex.
- SegmentResult is used for all operations; only iterators may throw.
- Maintenance work (flush/compact) is always executed on the provided executor.

## Phase 1: Decide ownership and contracts
- [ ] Document the final responsibilities of SegmentCore vs SegmentImpl.
- [x] Decide whether SegmentImplSynchronizationAdapter stays or moves to
      segmentindex.
- [x] Define the minimal API SegmentImpl needs from segmentindex to schedule
      maintenance (executor and callback contract).
- [x] Choose a temporary glue package name for segment↔segmentindex adapters
      and remove it once migration completes.
- [ ] Update docs/architecture/segment-concurency.md with the implementation
      mapping once agreed.

## Phase 2: SegmentImpl API shape
- [x] Add executor dependency to SegmentImpl and SegmentBuilder (constructor or
      setter path).
- [x] Split SegmentImpl maintenance into two layers:
      - public flush/compact: check state, set state, submit task
      - internal runFlush/runCompact: execute core.flush/core.compact and finish
        state transitions
- [ ] Ensure simple getters (stats/cache sizes/id) only delegate to SegmentCore.
- [ ] Ensure get/put check state and then delegate to SegmentCore.
- [ ] Ensure openIterator checks state and uses state machine rules only.

## Phase 3: Move async/scheduling to segmentindex
- [x] Move or replace segmentbridge classes (SegmentAsyncAdapter, scheduler,
      policies) into segmentindex.
- [ ] Remove futures and maintenance scheduling logic from segment package.
- [x] Place transitional boilerplate in a removable package and delete it
      after the migration.
- [x] Update SegmentRegistry to build SegmentImpl with executor and to use the
      new maintenance scheduling flow.
- [x] Update any callers that referenced segmentbridge package directly.

## Phase 4: Behavior and state transitions
- [ ] Verify state transitions for flush/compact are linearized and end in READY
      or ERROR.
- [ ] Ensure only one maintenance task is active per segment (serialized in
      segmentindex).
- [ ] Ensure failure paths always set ERROR and do not leave the segment in
      FREEZE or MAINTENANCE_RUNNING.

## Phase 5: Tests
- [ ] Add unit tests for SegmentImpl state transitions (flush/compact success,
      failure).
- [ ] Add tests for segmentindex maintenance scheduling and retry/backpressure.
- [ ] Update existing tests that used SegmentAsyncAdapter.
- [ ] Run mvn test and mvn verify -Ddependency-check.skip=true as validation.

## Phase 6: Cleanup
- [x] Remove obsolete classes in segmentbridge after migration.
- [ ] Remove or deprecate any old APIs that are no longer used.
- [x] Re-scan docs for outdated references to segmentbridge.
