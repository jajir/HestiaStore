package org.hestiastore.index.segment;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileLock;

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
    private final SegmentMaintenanceService maintenanceService;
    private final SegmentMaintenancePolicy<K, V> maintenancePolicy;
    private final Executor maintenanceExecutor;
    private final FileLock segmentLock;

    /**
     * Creates a segment implementation with the given core and executor.
     *
     * @param core segment core implementation
     * @param segmentCompacter compaction helper
     * @param maintenanceExecutor executor for maintenance tasks
     * @param maintenancePolicy maintenance decision policy
     */
    SegmentImpl(final SegmentCore<K, V> core,
            final SegmentCompacter<K, V> segmentCompacter,
            final Executor maintenanceExecutor,
            final SegmentMaintenancePolicy<K, V> maintenancePolicy) {
        this(core, segmentCompacter, maintenanceExecutor, maintenancePolicy,
                null);
    }

    /**
     * Creates a segment implementation with the given core and executor.
     *
     * @param core segment core implementation
     * @param segmentCompacter compaction helper
     * @param maintenanceExecutor executor for maintenance tasks
     * @param maintenancePolicy maintenance decision policy
     * @param segmentLock file lock held for the segment lifetime
     */
    SegmentImpl(final SegmentCore<K, V> core,
            final SegmentCompacter<K, V> segmentCompacter,
            final Executor maintenanceExecutor,
            final SegmentMaintenancePolicy<K, V> maintenancePolicy,
            final FileLock segmentLock) {
        this.core = Vldtn.requireNonNull(core, "core");
        this.segmentCompacter = Vldtn.requireNonNull(segmentCompacter,
                "segmentCompacter");
        this.maintenanceExecutor = Vldtn.requireNonNull(maintenanceExecutor,
                "maintenanceExecutor");
        this.maintenancePolicy = Vldtn.requireNonNull(maintenancePolicy,
                "maintenancePolicy");
        this.maintenanceService = new SegmentMaintenanceService(gate,
                this.maintenanceExecutor);
        this.segmentLock = segmentLock;
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
        return maintenanceService.startMaintenance(() -> {
            final List<Entry<K, V>> snapshotEntries = segmentCompacter
                    .prepareCompaction(core);
            final SegmentFullWriterTx<K, V> writerTx = core.openFullWriteTx();
            return new SegmentMaintenanceWork(
                    () -> segmentCompacter.writeCompaction(core,
                            snapshotEntries, writerTx),
                    () -> segmentCompacter.publishCompaction(core, writerTx));
        }, this::scheduleMaintenanceIfNeeded);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SegmentResult<Void> put(final K key, final V value) {
        if (!gate.tryEnterWrite()) {
            return resultForState(gate.getState());
        }
        final SegmentResult<Void> result;
        final boolean shouldScheduleMaintenance;
        try {
            if (!core.tryPutWithoutWaiting(key, value)) {
                result = SegmentResult.busy();
                shouldScheduleMaintenance = false;
            } else {
                result = SegmentResult.ok();
                shouldScheduleMaintenance = true;
            }
        } finally {
            gate.exitWrite();
        }
        if (shouldScheduleMaintenance) {
            scheduleMaintenanceIfNeeded();
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SegmentResult<CompletionStage<Void>> flush() {
        return maintenanceService.startMaintenance(() -> {
            final List<Entry<K, V>> entries = core.freezeWriteCacheForFlush();
            return new SegmentMaintenanceWork(
                    () -> core.flushFrozenWriteCacheToDeltaFile(entries),
                    core::applyFrozenWriteCacheAfterFlush);
        }, this::scheduleMaintenanceIfNeeded);
    }

    private void scheduleMaintenanceIfNeeded() {
        final SegmentMaintenanceDecision decision = maintenancePolicy
                .evaluateAfterWrite(this);
        if (decision.shouldCompact()) {
            compact();
            return;
        }
        if (decision.shouldFlush()) {
            flush();
        }
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
    public long getNumberOfKeysInSegmentCache() {
        return core.getNumberOfKeysInSegmentCache();
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
        gate.beginClose();
        gate.awaitIdleForClose();
        gate.forceClosed();
        try {
            core.close();
        } finally {
            if (segmentLock != null && segmentLock.isLocked()) {
                segmentLock.unlock();
            }
        }
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

    /**
     * Marks the segment ERROR unless it is already CLOSED.
     */
    private void failUnlessClosed() {
        if (gate.getState() != SegmentState.CLOSED) {
            gate.fail();
        }
    }

}
