# Segment Thread-Safety Refactor Checklist (Lock-Free)

## Decisions (confirm before implementation)
- [x] Segment contract states thread-safe by definition.
- [x] `get`/`put` can run concurrently in `READY` (and limited in
      `MAINTENANCE_RUNNING`).
- [x] Maintenance is exclusive only for admission/snapshot; IO runs in
      background.
- [x] No external locks or adapter required for safety.

## Contract and Documentation
- [x] Update `Segment.java` Javadoc with the thread-safe contract.
- [x] Update `segment-concurrency.md` with the lock-free safety model.
- [x] Update any legacy docs that still assume lock-based safety.

## Concurrency Gate (Core Mechanism)
- [x] Introduce `SegmentConcurrencyGate` (package-private).
- [x] Add in-flight counters for reads and writes.
- [x] Implement admission checks:
      - [x] `tryEnterRead/Write` checks state → increments counter →
            re-checks state (rollback on BUSY).
      - [x] `exitRead/Write` decrements counters.
- [x] Implement maintenance admission:
      - [x] CAS `READY -> FREEZE` (freeze admission).
      - [x] Refuse new ops (BUSY).
      - [x] Wait for in-flight counters to reach zero.
      - [x] Snapshot/freeze write cache and queue background IO.
      - [x] Release exclusivity; run IO in `MAINTENANCE_RUNNING`.
      - [x] On completion: publish new view, bump version, `READY`.

## SegmentImpl Wiring
- [x] Route `get/put/openIterator` through the gate.
- [x] Route `flush/compact` through freeze+drain before scheduling IO.
- [x] Ensure `SegmentResult<CompletionStage<Void>>` semantics:
      - [x] `OK` returns non-null stage (accepted).
      - [x] `BUSY/CLOSED/ERROR` return null stage (not started).
- [x] Ensure `openIterator(FULL_ISOLATION)` uses the same freeze+drain flow.

## SegmentCore Concurrency Safety
- [x] Write cache uses a thread-safe map.
- [x] Write cache size/counts are atomic.
- [x] Published view swap is atomic (publish under `FREEZE` after IO).
- [x] Version increment provides visibility for iterators.
- [x] Remove any shared mutable state without atomic protection.

## Iterator Behavior
- [x] `FAIL_FAST` checks version on each `hasNext/next`.
- [x] `FULL_ISOLATION` blocks new ops until iterator is closed.
- [x] Iterator close returns segment to `READY`.

## Legacy Adapter
- [x] Remove `SegmentImplSynchronizationAdapter` and
      `SegmentWriteLockSupport`.

## Tests (must be bulletproof)
### Unit Tests
- [x] Gate admission allows `get/put` in `READY`, refuses in `FREEZE`.
- [x] Maintenance admission drains in-flight counters before IO starts.
- [x] `flush/compact` stages complete on success, fail exceptionally on error.
- [x] `FULL_ISOLATION` blocks new ops until close.
- [x] `FAIL_FAST` stops/throws on version change.

### Concurrency Tests (JUnit)
- [x] Parallel `get` + `put` on same keys, no exceptions, final values correct.
- [x] Flush with concurrent writes: pre-freeze writes are flushed; post-freeze
      writes stay in cache and are visible to `get`.
- [x] Compact with concurrent reads: readers see consistent data.
- [x] Multiple concurrent `flush/compact`: one OK, others BUSY.

### Stress Tests (optional, recommended)
- [x] Mixed workload loop (put/get/flush/compact) with time limit and
      no deadlocks or data loss.

## Acceptance
- [x] All new tests pass reliably.
- [x] Tests proving that gets and puts can run concurrently.
- [x] No external locks required for thread safety.
- [x] Contract and docs explicitly state thread-safe behavior.
