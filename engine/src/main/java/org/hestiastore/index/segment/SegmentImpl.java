package org.hestiastore.index.segment;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Public segment implementation that delegates single-threaded work to
 * {@link SegmentCore}.
 *
 * @param <K> key type stored in this segment
 * @param <V> value type stored in this segment
 */
class SegmentImpl<K, V> implements Segment<K, V> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final SegmentCore<K, V> core;
    private final SegmentCompacter<K, V> segmentCompacter;
    private final SegmentConcurrencyGate gate = new SegmentConcurrencyGate();
    private final SegmentMaintenanceService maintenanceService;
    private final SegmentMaintenancePolicy<K, V> maintenancePolicy;
    private final Executor maintenanceExecutor;
    private final SegmentDirectoryLocking directoryLocking;
    private final LongAdder compactRequestCx = new LongAdder();
    private final LongAdder flushRequestCx = new LongAdder();

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
     * @param directoryLocking lock helper for the segment directory
     */
    SegmentImpl(final SegmentCore<K, V> core,
            final SegmentCompacter<K, V> segmentCompacter,
            final Executor maintenanceExecutor,
            final SegmentMaintenancePolicy<K, V> maintenancePolicy,
            final SegmentDirectoryLocking directoryLocking) {
        this.core = Vldtn.requireNonNull(core, "core");
        this.segmentCompacter = Vldtn.requireNonNull(segmentCompacter,
                "segmentCompacter");
        this.maintenanceExecutor = Vldtn.requireNonNull(maintenanceExecutor,
                "maintenanceExecutor");
        this.maintenancePolicy = Vldtn.requireNonNull(maintenancePolicy,
                "maintenancePolicy");
        this.maintenanceService = new SegmentMaintenanceService(gate,
                this.maintenanceExecutor, this::onMaintenanceFailure);
        this.directoryLocking = directoryLocking;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SegmentStats getStats() {
        final SegmentStats coreStats = core.getStats();
        return new SegmentStats(coreStats.getNumberOfKeysInDeltaCache(),
                coreStats.getNumberOfKeysInSegment(),
                coreStats.getNumberOfKeysInScarceIndex(),
                compactRequestCx.sum(), flushRequestCx.sum());
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
    public SegmentResult<Void> compact() {
        compactRequestCx.increment();
        final AtomicReference<SegmentCompacter.CompactionPlan<K, V>> planRef = new AtomicReference<>();
        return maintenanceService.startMaintenance(() -> {
            final SegmentCompacter.CompactionPlan<K, V> plan = segmentCompacter
                    .prepareCompactionPlan(core);
            planRef.set(plan);
            return new SegmentMaintenanceWork(
                    () -> segmentCompacter.writeCompaction(plan),
                    () -> segmentCompacter.publishCompaction(plan));
        }, () -> {
            segmentCompacter.scheduleCleanup(planRef.get(), maintenanceExecutor);
            scheduleMaintenanceIfNeeded();
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
    public SegmentResult<Void> flush() {
        flushRequestCx.increment();
        if (core.getDeltaCacheFileCount() >= core.getSegmentConf()
                .getMaxNumberOfDeltaCacheFiles()) {
            return compact();
        }
        return maintenanceService.startMaintenance(() -> {
            core.freezeWriteCacheForFlush();
            return new SegmentMaintenanceWork(
                    core::flushFrozenWriteCacheToDeltaFile,
                    core::applyFrozenWriteCacheAfterFlush);
        }, this::scheduleMaintenanceIfNeeded);
    }

    private void scheduleMaintenanceIfNeeded() {
        if (gate.getState() != SegmentState.READY) {
            return;
        }
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
    public int getNumberOfDeltaCacheFiles() {
        return core.getDeltaCacheFileCount();
    }

    @Override
    public long getBloomFilterRequestCount() {
        return core.getBloomFilterRequestCount();
    }

    @Override
    public long getBloomFilterRefusedCount() {
        return core.getBloomFilterRefusedCount();
    }

    @Override
    public long getBloomFilterPositiveCount() {
        return core.getBloomFilterPositiveCount();
    }

    @Override
    public long getBloomFilterFalsePositiveCount() {
        return core.getBloomFilterFalsePositiveCount();
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
     * Starts closing the segment in the maintenance thread.
     */
    @Override
    public SegmentResult<Void> close() {
        if (!gate.tryEnterCloseAndDrain()) {
            return resultForState(gate.getState());
        }
        core.freezeWriteCacheForFlush();
        try {
            maintenanceExecutor.execute(this::runClose);
        } catch (final RuntimeException e) {
            failUnlessClosed();
            return SegmentResult.error();
        }
        return SegmentResult.ok();
    }

    private void runClose() {
        try {
            core.flushFrozenWriteCacheToDeltaFile();
            core.applyFrozenWriteCacheAfterFlush();
            core.close();
            if (!gate.finishCloseToClosed()) {
                gate.fail();
            }
        } catch (final RuntimeException e) {
            gate.fail();
        } finally {
            if (directoryLocking != null) {
                directoryLocking.unlock();
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

    private void onMaintenanceFailure(final Throwable failure) {
        logger.error("Segment '{}' maintenance failed.", core.getId(), failure);
    }

}
