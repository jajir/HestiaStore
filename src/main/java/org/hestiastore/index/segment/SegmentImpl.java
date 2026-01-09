package org.hestiastore.index.segment;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

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
    private final SegmentConcurrencyGate gate = new SegmentConcurrencyGate();
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
            if (!gate.tryEnterFreezeAndDrain()) {
                return resultForState(gate.getState());
            }
            try {
                core.invalidateIterators();
                final EntryIterator<K, V> iterator = core
                        .openIterator(isolation);
                return SegmentResult.ok(
                        new ExclusiveAccessIterator<>(iterator, gate));
            } catch (final RuntimeException e) {
                failUnlessClosed();
                return SegmentResult.error();
            }
        }
        if (!gate.tryEnterRead()) {
            return resultForState(gate.getState());
        }
        try {
            return SegmentResult.ok(core.openIterator(isolation));
        } catch (final RuntimeException e) {
            failUnlessClosed();
            return SegmentResult.error();
        } finally {
            gate.exitRead();
        }
    }

    @Override
    public SegmentResult<CompletionStage<Void>> compact() {
        return startMaintenance(() -> {
            final List<Entry<K, V>> snapshotEntries = segmentCompacter
                    .prepareCompaction(core);
            return () -> segmentCompacter.compact(core, snapshotEntries);
        });
    }

    @Override
    public SegmentResult<Void> put(final K key, final V value) {
        if (!gate.tryEnterWrite()) {
            return resultForState(gate.getState());
        }
        try {
            if (!core.tryPutWithoutWaiting(key, value)) {
                return SegmentResult.busy();
            }
            return SegmentResult.ok();
        } finally {
            gate.exitWrite();
        }
    }

    @Override
    public SegmentResult<CompletionStage<Void>> flush() {
        return startMaintenance(() -> {
            final List<Entry<K, V>> entries = core.freezeWriteCacheForFlush();
            return () -> {
                core.flushFrozenWriteCacheToDeltaFile(entries);
                core.applyFrozenWriteCacheAfterFlush();
            };
        });
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
        if (!gate.tryEnterRead()) {
            return resultForState(gate.getState());
        }
        try {
            return SegmentResult.ok(core.get(key));
        } finally {
            gate.exitRead();
        }
    }

    @Override
    public SegmentId getId() {
        return core.getId();
    }

    @Override
    public SegmentState getState() {
        return gate.getState();
    }

    @Override
    protected void doClose() {
        gate.forceClosed();
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
            final Supplier<Runnable> workSupplier) {
        Vldtn.requireNonNull(workSupplier, "workSupplier");
        if (!gate.tryEnterFreezeAndDrain()) {
            return resultForState(gate.getState());
        }
        final Runnable work;
        try {
            work = Vldtn.requireNonNull(workSupplier.get(), "work");
        } catch (final RuntimeException e) {
            failUnlessClosed();
            return SegmentResult.error();
        }
        if (!gate.enterMaintenanceRunning()) {
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
        if (gate.getState() == SegmentState.CLOSED) {
            completion.complete(null);
            return;
        }
        if (!gate.finishMaintenanceToFreeze()) {
            failUnlessClosed();
            completion.completeExceptionally(new IllegalStateException(
                    "Segment maintenance failed to transition to FREEZE."));
            return;
        }
        if (!gate.finishFreezeToReady()) {
            failUnlessClosed();
            completion.completeExceptionally(new IllegalStateException(
                    "Segment maintenance failed to transition to READY."));
            return;
        }
        completion.complete(null);
    }

    private void failUnlessClosed() {
        if (gate.getState() != SegmentState.CLOSED) {
            gate.fail();
        }
    }

}
