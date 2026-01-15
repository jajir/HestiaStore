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
class SegmentImpl<K, V> extends AbstractCloseableResource
        implements Segment<K, V> {

    private final SegmentCore<K, V> core;
    private final SegmentCompacter<K, V> segmentCompacter;
    private final SegmentConcurrencyGate gate = new SegmentConcurrencyGate();
    private final Executor maintenanceExecutor;

    /**
     * Creates a segment implementation with the given core and executor.
     *
     * @param core segment core implementation
     * @param segmentCompacter compaction helper
     * @param maintenanceExecutor executor for maintenance tasks
     */
    SegmentImpl(final SegmentCore<K, V> core,
            final SegmentCompacter<K, V> segmentCompacter,
            final Executor maintenanceExecutor) {
        this.core = Vldtn.requireNonNull(core, "core");
        this.segmentCompacter = Vldtn.requireNonNull(segmentCompacter,
                "segmentCompacter");
        this.maintenanceExecutor = Vldtn.requireNonNull(maintenanceExecutor,
                "maintenanceExecutor");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SegmentStats getStats() {
        return core.getStats();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getNumberOfKeys() {
        return core.getNumberOfKeys();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public K checkAndRepairConsistency() {
        final SegmentConsistencyChecker<K, V> consistencyChecker = new SegmentConsistencyChecker<>(
                this, core.getKeyComparator());
        return consistencyChecker.checkAndRepairConsistency();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void invalidateIterators() {
        core.invalidateIterators();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SegmentResult<EntryIterator<K, V>> openIterator() {
        return openIterator(SegmentIteratorIsolation.FAIL_FAST);
    }

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
    @Override
    public SegmentResult<CompletionStage<Void>> compact() {
        return startMaintenance(() -> {
            final List<Entry<K, V>> snapshotEntries = segmentCompacter
                    .prepareCompaction(core);
            final SegmentFullWriterTx<K, V> writerTx = core.openFullWriteTx();
            return new MaintenanceWork(
                    () -> segmentCompacter.writeCompaction(core,
                            snapshotEntries, writerTx),
                    () -> segmentCompacter.publishCompaction(core, writerTx));
        });
    }

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
    @Override
    public SegmentResult<CompletionStage<Void>> flush() {
        return startMaintenance(() -> {
            final List<Entry<K, V>> entries = core.freezeWriteCacheForFlush();
            return new MaintenanceWork(
                    () -> core.flushFrozenWriteCacheToDeltaFile(entries),
                    core::applyFrozenWriteCacheAfterFlush);
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumberOfKeysInWriteCache() {
        return core.getNumberOfKeysInWriteCache();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getNumberOfKeysInCache() {
        return core.getNumberOfKeysInCache();
    }

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
    @Override
    public SegmentId getId() {
        return core.getId();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SegmentState getState() {
        return gate.getState();
    }

    /**
     * Closes the segment and marks it CLOSED.
     */
    @Override
    protected void doClose() {
        gate.forceClosed();
        core.close();
    }

    /**
     * Maps the current state to a SegmentResult with no value.
     *
     * @param state segment state
     * @param <T> result value type
     * @return mapped result
     */
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

    private static final class MaintenanceWork {

        private final Runnable ioWork;
        private final Runnable publishWork;

        /**
         * Creates a maintenance work bundle.
         *
         * @param ioWork IO phase runnable
         * @param publishWork publish phase runnable
         */
        private MaintenanceWork(final Runnable ioWork,
                final Runnable publishWork) {
            this.ioWork = Vldtn.requireNonNull(ioWork, "ioWork");
            this.publishWork = Vldtn.requireNonNull(publishWork, "publishWork");
        }

        /**
         * Returns the IO phase task.
         *
         * @return IO phase runnable
         */
        private Runnable ioWork() {
            return ioWork;
        }

        /**
         * Returns the publish phase task.
         *
         * @return publish phase runnable
         */
        private Runnable publishWork() {
            return publishWork;
        }
    }

    /**
     * Starts a maintenance operation by freezing the segment and running work.
     *
     * @param workSupplier supplier of maintenance tasks
     * @return result with completion stage
     */
    private SegmentResult<CompletionStage<Void>> startMaintenance(
            final Supplier<MaintenanceWork> workSupplier) {
        Vldtn.requireNonNull(workSupplier, "workSupplier");
        if (!gate.tryEnterFreezeAndDrain()) {
            return resultForState(gate.getState());
        }
        final MaintenanceWork work;
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

    /**
     * Executes maintenance work and transitions the gate through states.
     *
     * @param work maintenance work bundle
     * @param completion completion stage to resolve
     */
    private void runMaintenance(final MaintenanceWork work,
            final CompletableFuture<Void> completion) {
        try {
            work.ioWork().run();
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
        try {
            work.publishWork().run();
        } catch (final RuntimeException e) {
            failUnlessClosed();
            completion.completeExceptionally(e);
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

    /**
     * Marks the segment ERROR unless it is already CLOSED.
     */
    private void failUnlessClosed() {
        if (gate.getState() != SegmentState.CLOSED) {
            gate.fail();
        }
    }

}
