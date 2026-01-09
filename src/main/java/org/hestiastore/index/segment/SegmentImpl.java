package org.hestiastore.index.segment;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;

/**
 * Public segment implementation that delegates single-threaded work to
 * {@link SegmentCore}.
 *
 * @param <K> key type stored in this segment
 * @param <V> value type stored in this segment
 */
public class SegmentImpl<K, V> extends AbstractCloseableResource
        implements Segment<K, V> {

    private final SegmentCore<K, V> core;
    private final SegmentCompacter<K, V> segmentCompacter;
    private final SegmentStateMachine stateMachine = new SegmentStateMachine();
    private final Executor maintenanceExecutor;

    SegmentImpl(final SegmentCore<K, V> core,
            final SegmentCompacter<K, V> segmentCompacter,
            final Executor maintenanceExecutor) {
        this.core = Vldtn.requireNonNull(core, "core");
        this.segmentCompacter = Vldtn.requireNonNull(segmentCompacter,
                "segmentCompacter");
        this.maintenanceExecutor = Vldtn.requireNonNull(maintenanceExecutor,
                "maintenanceExecutor");
    }

    @Override
    public SegmentStats getStats() {
        return core.getStats();
    }

    @Override
    public long getNumberOfKeys() {
        return core.getNumberOfKeys();
    }

    @Override
    public K checkAndRepairConsistency() {
        final SegmentConsistencyChecker<K, V> consistencyChecker = new SegmentConsistencyChecker<>(
                this, core.getKeyComparator());
        return consistencyChecker.checkAndRepairConsistency();
    }

    @Override
    public void invalidateIterators() {
        core.invalidateIterators();
    }

    @Override
    public SegmentResult<EntryIterator<K, V>> openIterator() {
        return openIterator(SegmentIteratorIsolation.FAIL_FAST);
    }

    @Override
    public SegmentResult<EntryIterator<K, V>> openIterator(
            final SegmentIteratorIsolation isolation) {
        Vldtn.requireNonNull(isolation, "isolation");
        if (isolation == SegmentIteratorIsolation.FULL_ISOLATION) {
            if (!stateMachine.tryEnterFreeze()) {
                return resultForState(stateMachine.getState());
            }
            try {
                core.invalidateIterators();
                final EntryIterator<K, V> iterator = core
                        .openIterator(isolation);
                return SegmentResult.ok(
                        new ExclusiveAccessIterator<>(iterator, stateMachine));
            } catch (final RuntimeException e) {
                failUnlessClosed();
                return SegmentResult.error();
            }
        }
        final SegmentState state = stateMachine.getState();
        if (state != SegmentState.READY
                && state != SegmentState.MAINTENANCE_RUNNING) {
            return resultForState(state);
        }
        try {
            return SegmentResult.ok(core.openIterator(isolation));
        } catch (final RuntimeException e) {
            failUnlessClosed();
            return SegmentResult.error();
        }
    }

    @Override
    public SegmentResult<CompletionStage<Void>> compact() {
        return startMaintenance(() -> segmentCompacter.forceCompact(core));
    }

    @Override
    public SegmentResult<Void> put(final K key, final V value) {
        final SegmentState state = stateMachine.getState();
        if (state != SegmentState.READY
                && state != SegmentState.MAINTENANCE_RUNNING) {
            return resultForState(state);
        }
        if (!core.tryPutWithoutWaiting(key, value)) {
            return SegmentResult.busy();
        }
        return SegmentResult.ok();
    }

    @Override
    public SegmentResult<CompletionStage<Void>> flush() {
        return startMaintenance(core::flush);
    }

    @Override
    public int getNumberOfKeysInWriteCache() {
        return core.getNumberOfKeysInWriteCache();
    }

    @Override
    public long getNumberOfKeysInCache() {
        return core.getNumberOfKeysInCache();
    }

    @Override
    public SegmentResult<V> get(final K key) {
        final SegmentState state = stateMachine.getState();
        if (state == SegmentState.READY
                || state == SegmentState.MAINTENANCE_RUNNING) {
            return SegmentResult.ok(core.get(key));
        }
        return resultForState(state);
    }

    @Override
    public SegmentId getId() {
        return core.getId();
    }

    @Override
    public SegmentState getState() {
        return stateMachine.getState();
    }

    @Override
    protected void doClose() {
        stateMachine.forceClosed();
        core.close();
    }

    private static <T> SegmentResult<T> resultForState(
            final SegmentState state) {
        if (state == SegmentState.CLOSED) {
            return SegmentResult.closed();
        }
        if (state == SegmentState.ERROR) {
            return SegmentResult.error();
        }
        return SegmentResult.busy();
    }

    private SegmentResult<CompletionStage<Void>> startMaintenance(
            final Runnable work) {
        if (!stateMachine.tryEnterFreeze()) {
            return resultForState(stateMachine.getState());
        }
        if (!stateMachine.enterMaintenanceRunning()) {
            failUnlessClosed();
            return SegmentResult.error();
        }
        final CompletableFuture<Void> completion = new CompletableFuture<>();
        try {
            maintenanceExecutor.execute(
                    () -> runMaintenance(work, completion));
        } catch (final RuntimeException e) {
            failUnlessClosed();
            return SegmentResult.error();
        }
        return SegmentResult.ok(completion);
    }

    private void runMaintenance(final Runnable work,
            final CompletableFuture<Void> completion) {
        try {
            work.run();
        } catch (final RuntimeException e) {
            failUnlessClosed();
            completion.completeExceptionally(e);
            return;
        }
        if (stateMachine.getState() == SegmentState.CLOSED) {
            completion.complete(null);
            return;
        }
        if (!stateMachine.finishMaintenanceToFreeze()) {
            failUnlessClosed();
            completion.completeExceptionally(new IllegalStateException(
                    "Segment maintenance failed to transition to FREEZE."));
            return;
        }
        if (!stateMachine.finishFreezeToReady()) {
            failUnlessClosed();
            completion.completeExceptionally(new IllegalStateException(
                    "Segment maintenance failed to transition to READY."));
            return;
        }
        completion.complete(null);
    }

    private void failUnlessClosed() {
        if (stateMachine.getState() != SegmentState.CLOSED) {
            stateMachine.fail();
        }
    }

}
