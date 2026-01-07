# Segment Refactor Checklist

1) [x] Confirm contract is final in `docs/architecture/segment-concurency.md`.
2) [x] Inventory current segment classes and call sites.
3) [x] Create `Segment` public interface (API parity with current segment).
4) [x] Extract current single-threaded logic into `SegmentCore`.
5) [x] Add `SegmentImpl` that delegates to `SegmentCore` (no concurrency logic yet).
6) [x] Switch call sites to depend on `Segment` interface (use adapter only at edges).
7) [x] Implement `SegmentStateMachine` with serialized transitions.
8) [x] Gate `SegmentImpl` operations via the state machine (return `BUSY`/`CLOSED`/`ERROR`).
9) [x] Implement `MaintenanceController` to run `flush()`/`compact()` in background.
    - [x] Higher-level application must provide an `ExecutorService` for maintenance submissions.
10) [x] Add automatic triggers: write cache full → `flush()`, delta cache full → `compact()`.
11) [x] Enforce `EXCLUSIVE_ACCESS` lifecycle and iterator invalidation on version change.
12) [ ] Tests:
    - [x] State transitions basics.
    - [x] BUSY reasons.
    - [x] Version bump timing + iterators.
    - [x] EXCLUSIVE_ACCESS lifecycle.
    - [x] Auto triggers (write cache + delta cache thresholds).
13) [x] Remove legacy direct entry points/adapters.
14) [ ] Run `mvn verify` and fix regressions.
    - [ ] `mvn verify` timed out locally; rerun with longer timeout.
15) [x] Update docs/diagrams as needed.
