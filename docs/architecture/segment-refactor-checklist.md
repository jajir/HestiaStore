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
- [x] Document the final responsibilities of SegmentCore vs SegmentImpl.
- [x] Decide whether SegmentImplSynchronizationAdapter stays or moves to
      segmentindex.
- [x] Define the minimal API SegmentImpl needs from segmentindex to schedule
      maintenance (executor and callback contract).
- [x] Choose a temporary glue package name for segment↔segmentindex adapters
      and remove it once migration completes.
- [x] Update docs/architecture/segment-concurency.md with the implementation
      mapping once agreed.

## Phase 2: SegmentImpl API shape
- [x] Add executor dependency to SegmentImpl and SegmentBuilder (constructor or
      setter path).
- [x] Split SegmentImpl maintenance into two layers:
      - public flush/compact: check state, set state, submit task
      - internal runFlush/runCompact: execute core.flush/core.compact and finish
        state transitions
- [x] Ensure simple getters (stats/cache sizes/id) only delegate to SegmentCore.
- [x] Ensure get/put check state and then delegate to SegmentCore.
- [x] Ensure openIterator checks state and uses state machine rules only.

## Phase 3: Move async/scheduling to segmentindex
- [x] Move or replace legacy bridge classes (async adapter, scheduler, policies)
      into segmentindex.
- [x] Remove futures and maintenance scheduling logic from segment package.
- [x] Place transitional boilerplate in a removable package and delete it
      after the migration.
- [x] Update SegmentRegistry to build SegmentImpl with executor and to use the
      new maintenance scheduling flow.
- [x] Update any callers that referenced the legacy bridge package directly.

## Phase 4: Behavior and state transitions
- [x] Verify state transitions for flush/compact are linearized and end in READY
      or ERROR.
- [x] Ensure only one maintenance task is active per segment (serialized in
      segmentindex).
- [x] Ensure failure paths always set ERROR and do not leave the segment in
      FREEZE or MAINTENANCE_RUNNING.

Notes:
- Closing during maintenance leaves the segment in CLOSED, not READY/ERROR.
  Confirmed: CLOSED is an allowed terminal state.

## Phase 5: Tests
- [x] Add unit tests for SegmentImpl state transitions (flush/compact success,
      failure).
- [x] Add tests for segmentindex maintenance scheduling and retry/backpressure.
- [x] Update existing tests that used SegmentAsyncAdapter.
- [ ] Run mvn test and mvn verify -Ddependency-check.skip=true as validation.
Note: IntegrationSegmentIndexConsistencyTest and IntegrationSegmentIndexIteratorTest pass in isolation; full mvn test still times out at 180s.

## Phase 6: Cleanup
- [x] Remove obsolete legacy bridge classes after migration.
- [x] Remove or deprecate any old APIs that are no longer used.
- [x] Re-scan docs for outdated references to the legacy bridge package.
