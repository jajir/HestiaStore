# Segment Refactor Checklist

1) [x] Confirm contract is final in `docs/architecture/segment-concurency.md`.
2) [x] Inventory current segment classes and call sites.
3) [x] Create `Segment` public interface (API parity with current segment).
4) [x] Extract current single-threaded logic into `SegmentCore`.
5) [x] Add `SegmentImpl` that delegates to `SegmentCore` (no concurrency logic yet).
6) [ ] Switch call sites to depend on `Segment` interface (use adapter if needed).
7) [x] Implement `SegmentStateMachine` with serialized transitions.
8) [ ] Gate `SegmentImpl` operations via the state machine (return `BUSY`/`CLOSED`/`ERROR`).
9) [ ] Implement `MaintenanceController` to run `flush()`/`compact()` in background.
10) [ ] Add automatic triggers: write cache full → `flush()`, delta cache full → `compact()`.
11) [ ] Enforce `EXCLUSIVE_ACCESS` lifecycle and iterator invalidation on version change.
12) [ ] Tests: transitions + BUSY reasons; version bump timing + iterators; EXCLUSIVE_ACCESS; auto triggers.
13) [ ] Remove legacy direct entry points/adapters.
14) [ ] Run `mvn verify` and fix regressions.
15) [ ] Update docs/diagrams as needed.
