# Segment Thread-Safety Refactor Checklist (Lock-Free)

## Decisions (confirm before implementation)
- [ ] Segment contract states thread-safe by definition.
- [ ] `get`/`put` can run concurrently in `READY` (and limited in
      `MAINTENANCE_RUNNING`).
- [ ] Maintenance is exclusive only for admission/snapshot; IO runs in
      background.
- [ ] No external locks or adapter required for safety.

## Contract and Documentation
- [ ] Update `Segment.java` Javadoc with the thread-safe contract.
- [ ] Update `segment-concurency.md` with the lock-free safety model.
- [ ] Update any legacy docs that still assume lock-based safety.

## Concurrency Gate (Core Mechanism)
- [ ] Introduce `SegmentConcurrencyGate` (package-private).
- [ ] Add in-flight counters for reads and writes.
- [ ] Implement admission checks:
      - [ ] `tryEnterRead/Write` checks state → increments counter →
            re-checks state (rollback on BUSY).
      - [ ] `exitRead/Write` decrements counters.
- [ ] Implement maintenance admission:
      - [ ] CAS `READY -> FREEZE_REQUESTED`.
      - [ ] Refuse new ops (BUSY).
      - [ ] Wait for in-flight counters to reach zero.
      - [ ] Snapshot/freeze write cache and queue background IO.
      - [ ] Release exclusivity; run IO in `MAINTENANCE_RUNNING`.
      - [ ] On completion: publish new view, bump version, `READY`.

## SegmentImpl Wiring
- [ ] Route `get/put/openIterator` through the gate.
- [ ] Route `flush/compact` through freeze+drain before scheduling IO.
- [ ] Ensure `SegmentResult<CompletionStage<Void>>` semantics:
      - [ ] `OK` returns non-null stage (accepted).
      - [ ] `BUSY/CLOSED/ERROR` return null stage (not started).
- [ ] Ensure `openIterator(FULL_ISOLATION)` uses the same freeze+drain flow.

## SegmentCore Concurrency Safety
- [ ] Write cache uses a thread-safe map.
- [ ] Write cache size/counts are atomic.
- [ ] Published view swap is atomic (immutable snapshot + atomic reference).
- [ ] Version increment provides visibility for iterators.
- [ ] Remove any shared mutable state without atomic protection.

## Iterator Behavior
- [ ] `FAIL_FAST` checks version on each `hasNext/next`.
- [ ] `FULL_ISOLATION` blocks new ops until iterator is closed.
- [ ] Iterator close returns segment to `READY`.

## Legacy Adapter
- [ ] Deprecate or remove `SegmentImplSynchronizationAdapter` once
      `SegmentImpl` is fully thread-safe.
- [ ] If retained, document it as optional conservative serialization.

## Tests (must be bulletproof)
### Unit Tests
- [ ] Gate admission allows `get/put` in `READY`, refuses in `FREEZE`.
- [ ] Maintenance admission drains in-flight counters before IO starts.
- [ ] `flush/compact` stages complete on success, fail exceptionally on error.
- [ ] `FULL_ISOLATION` blocks new ops until close.
- [ ] `FAIL_FAST` stops/throws on version change.

### Concurrency Tests (JUnit)
- [ ] Parallel `get` + `put` on same keys, no exceptions, final values correct.
- [ ] Flush with concurrent writes: pre-freeze writes are flushed; post-freeze
      writes stay in cache and are visible to `get`.
- [ ] Compact with concurrent reads: readers see consistent data.
- [ ] Multiple concurrent `flush/compact`: one OK, others BUSY.

### Stress Tests (optional, recommended)
- [ ] Mixed workload loop (put/get/flush/compact) with time limit and
      no deadlocks or data loss.

## Acceptance
- [ ] All new tests pass reliably.
- [ ] No external locks required for thread safety.
- [ ] Contract and docs explicitly state thread-safe behavior.
