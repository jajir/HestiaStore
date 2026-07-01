package org.hestiastore.index.segment;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.LongAdder;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.OperationResult;
import org.hestiastore.index.OperationStatus;
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

    /** {@inheritDoc} */
    @Override
    public SegmentRuntimeSnapshot getRuntimeSnapshot() {
        final SegmentStats stats = core.getStats();
        return new SegmentRuntimeSnapshot(core.getId(), gate.getState(),
                stats.getNumberOfKeysInDeltaCache(),
                stats.getNumberOfKeysInSegment(),
                stats.getNumberOfKeysInScarceIndex(),
                core.getNumberOfKeysInSegmentCache(),
                core.getNumberOfKeysInWriteCache(),
                core.getDeltaCacheFileCount(),
                compactRequestCx.sum(), flushRequestCx.sum(),
                core.getBloomFilterRequestCount(),
                core.getBloomFilterRefusedCount(),
                core.getBloomFilterPositiveCount(),
                core.getBloomFilterFalsePositiveCount());
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
    public OperationResult<EntryIterator<K, V>> openIterator() {
        return openIterator(SegmentIteratorIsolation.FAIL_FAST);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OperationResult<EntryIterator<K, V>> openIterator(
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
                return OperationResult.ok(
                        new ExclusiveAccessIterator<>(iterator, gate));
            } catch (final RuntimeException e) {
                failUnlessClosed();
                return OperationResult.error();
            }
        }
        if (!gate.tryEnterRead()) {
            return resultForState(gate.getState());
        }
        try {
            return OperationResult.ok(core.openIterator(isolation));
        } catch (final RuntimeException e) {
            failUnlessClosed();
            return OperationResult.error();
        } finally {
            gate.exitRead();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OperationResult<Void> compact() {
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "Compact requested: segment='{}' state='{}' deltaFiles='{}' writeCacheKeys='{}' keysInCache='{}'",
                    core.getId(), gate.getState(), core.getDeltaCacheFileCount(),
                    core.getNumberOfKeysInWriteCache(),
                    core.getNumberOfKeysInCache());
        }
        final OperationResult<Void> result = scheduleCompaction();
        if (logger.isDebugEnabled()) {
            logger.debug("Compact scheduling finished: segment='{}' status='{}'",
                    core.getId(), result.getStatus());
        }
        if (result.getStatus() == OperationStatus.OK) {
            compactRequestCx.increment();
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OperationResult<Void> put(final K key, final V value) {
        if (!gate.tryEnterWrite()) {
            return resultForState(gate.getState());
        }
        final OperationResult<Void> result;
        final boolean writeAccepted;
        try {
            if (!core.tryPutWithoutWaiting(key, value)) {
                result = OperationResult.busy();
                writeAccepted = false;
            } else {
                result = OperationResult.ok();
                writeAccepted = true;
            }
        } finally {
            gate.exitWrite();
        }
        scheduleMaintenanceIfNeeded();
        if (!writeAccepted && !maintenancePolicy.canScheduleMaintenance()) {
            throw new IndexException(
                    "Write cache is full for segment '" + core.getId()
                            + "' and automatic maintenance is disabled.");
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OperationResult<Void> flush() {
        flushRequestCx.increment();
        final int deltaFileCount = core.getDeltaCacheFileCount();
        final int maxDeltaFileCount = core.getSegmentConf()
                .getMaxNumberOfDeltaCacheFiles();
        if (deltaFileCount >= maxDeltaFileCount) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Flush escalated to compact: segment='{}' deltaFiles='{}' maxDeltaFiles='{}'",
                        core.getId(), deltaFileCount, maxDeltaFileCount);
            }
            return compact();
        }
        final OperationResult<Void> result = scheduleFlush();
        if (logger.isDebugEnabled()
                && result.getStatus() == OperationStatus.OK) {
            logger.debug(
                    "Flush started: segment='{}' state='{}' deltaFiles='{}' maxDeltaFiles='{}' writeCacheKeys='{}'",
                    core.getId(), gate.getState(), deltaFileCount,
                    maxDeltaFileCount, core.getNumberOfKeysInWriteCache());
        }
        return result;
    }

    private void scheduleMaintenanceIfNeeded() {
        if (gate.isClosing() || gate.getState() != SegmentState.READY) {
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

    private OperationResult<Void> scheduleCompaction() {
        if (!gate.tryEnterFreezeAndDrain()) {
            return resultForState(gate.getState());
        }
        final SegmentCompacter.CompactionPlan<K, V> plan;
        try {
            plan = segmentCompacter.prepareCompactionPlan(core);
        } catch (final RuntimeException e) {
            failMaintenance(e);
            return OperationResult.error();
        }
        if (!gate.enterMaintenanceRunning()) {
            failUnlessClosed();
            return OperationResult.error();
        }
        try {
            maintenanceExecutor.execute(() -> executeScheduledCompaction(plan));
        } catch (final RuntimeException e) {
            failMaintenance(e);
            return OperationResult.error();
        }
        return OperationResult.ok();
    }

    private OperationResult<Void> scheduleFlush() {
        if (!gate.tryEnterFreezeAndDrain()) {
            return resultForState(gate.getState());
        }
        try {
            core.freezeWriteCacheForFlush();
        } catch (final RuntimeException e) {
            failMaintenance(e);
            return OperationResult.error();
        }
        if (!gate.enterMaintenanceRunning()) {
            failUnlessClosed();
            return OperationResult.error();
        }
        try {
            maintenanceExecutor.execute(this::executeScheduledFlush);
        } catch (final RuntimeException e) {
            failMaintenance(e);
            return OperationResult.error();
        }
        return OperationResult.ok();
    }

    private void executeScheduledCompaction(
            final SegmentCompacter.CompactionPlan<K, V> plan) {
        try {
            segmentCompacter.writeCompaction(plan);
        } catch (final RuntimeException e) {
            failMaintenance(e);
            return;
        }
        if (!finishMaintenanceToFreeze()) {
            return;
        }
        try {
            segmentCompacter.publishCompaction(plan);
        } catch (final RuntimeException e) {
            failMaintenance(e);
            return;
        }
        if (!finishFreezeToReady()) {
            return;
        }
        try {
            segmentCompacter.cleanupCompaction(plan);
        } catch (final RuntimeException e) {
            onMaintenanceFailure(e);
            gate.fail();
            return;
        }
        scheduleMaintenanceAfterReady();
    }

    private void executeScheduledFlush() {
        try {
            core.flushFrozenWriteCacheToDeltaFile();
        } catch (final RuntimeException e) {
            failMaintenance(e);
            return;
        }
        if (!finishMaintenanceToFreeze()) {
            return;
        }
        try {
            core.applyFrozenWriteCacheAfterFlush();
        } catch (final RuntimeException e) {
            failMaintenance(e);
            return;
        }
        if (finishFreezeToReady()) {
            scheduleMaintenanceAfterReady();
        }
    }

    private boolean finishMaintenanceToFreeze() {
        if (gate.getState() == SegmentState.CLOSED) {
            return false;
        }
        if (gate.finishMaintenanceToFreeze()) {
            return true;
        }
        onMaintenanceFailure(new IllegalStateException(
                "Maintenance gate failed to transition to FREEZE."));
        failUnlessClosed();
        return false;
    }

    private boolean finishFreezeToReady() {
        if (gate.finishFreezeToReady()) {
            return true;
        }
        onMaintenanceFailure(new IllegalStateException(
                "Maintenance gate failed to transition to READY."));
        failUnlessClosed();
        return false;
    }

    private void scheduleMaintenanceAfterReady() {
        try {
            scheduleMaintenanceIfNeeded();
        } catch (final RuntimeException e) {
            onMaintenanceFailure(e);
            gate.fail();
        }
    }

    private void failMaintenance(final RuntimeException e) {
        onMaintenanceFailure(e);
        failUnlessClosed();
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

    /** {@inheritDoc} */
    @Override
    public void applyRuntimeLimits(final SegmentRuntimeLimits limits) {
        Vldtn.requireNonNull(limits, "limits");
        core.applyRuntimeLimits(limits);
        if (maintenancePolicy instanceof SegmentMaintenancePolicyThreshold<?, ?> threshold) {
            threshold.updateThresholds(
                    limits.maxNumberOfKeysInSegmentCache(),
                    limits.maxNumberOfKeysInSegmentWriteCache());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OperationResult<V> get(final K key) {
        if (!gate.tryEnterRead()) {
            return resultForState(gate.getState());
        }
        try {
            return OperationResult.ok(core.get(key));
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
     * Closes the segment synchronously on the caller thread.
     */
    @Override
    public OperationResult<Void> close() {
        if (!gate.tryEnterCloseAndDrain()) {
            return resultForState(gate.getState());
        }
        core.freezeWriteCacheForFlush();
        if (!closeInternal()) {
            return OperationResult.error();
        }
        return OperationResult.ok();
    }

    private boolean closeInternal() {
        try {
            core.flushFrozenWriteCacheToDeltaFile();
            core.applyFrozenWriteCacheAfterFlush();
            core.close();
            if (!gate.finishCloseToClosed()) {
                gate.fail();
                return false;
            }
            releaseDirectoryLock();
            return true;
        } catch (final RuntimeException e) {
            gate.fail();
            return false;
        }
    }

    private void releaseDirectoryLock() {
        if (directoryLocking != null) {
            directoryLocking.unlock();
        }
    }

    /**
     * Maps the current state to a SegmentResult with no value.
     *
     * @param state segment state
     * @param <T> result value type
     * @return mapped result
     */
    private static <T> OperationResult<T> resultForState(
            final SegmentState state) {
        if (state == SegmentState.CLOSED) {
            return OperationResult.closed();
        }
        if (state == SegmentState.ERROR) {
            return OperationResult.error();
        }
        return OperationResult.busy();
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
