package org.hestiastore.index.segmentindex.core.split;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopology;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopology.RouteDrain;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopology.RouteDrainResult;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentHandle;

/**
 * Default implementation of split scheduling and split-publish admission.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SplitExecutionCoordinatorImpl<K, V>
        implements SplitExecutionCoordinator<K, V> {

    private static final long SPLIT_RESCHEDULE_COOLDOWN_MILLIS = 500L;
    private static final long SPLIT_RESCHEDULE_COOLDOWN_NANOS = TimeUnit.MILLISECONDS
            .toNanos(SPLIT_RESCHEDULE_COOLDOWN_MILLIS);
    private static final long SPLIT_RESCHEDULE_COOLDOWN_MAX_MILLIS = 5_000L;
    private static final long SPLIT_RESCHEDULE_COOLDOWN_MAX_NANOS = TimeUnit.MILLISECONDS
            .toNanos(SPLIT_RESCHEDULE_COOLDOWN_MAX_MILLIS);
    private static final long SPLIT_RETRY_GROWTH_MIN_KEYS = 8L;
    private static final long SPLIT_RETRY_GROWTH_DIVISOR = 10L;
    private static final long SPLIT_RETRY_GROWTH_MAX_KEYS = 1_024L;
    private static final long SPLIT_COOLDOWN_OBSERVED_MULTIPLIER = 2L;
    private static final long SPLIT_COOLDOWN_SMOOTHING_WEIGHT_PREVIOUS = 3L;
    private static final long SPLIT_COOLDOWN_SMOOTHING_WEIGHT_OBSERVED = 1L;

    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final SegmentTopology<K> segmentTopology;
    private final RouteSplitCoordinator<K, V> routeSplitCoordinator;
    private final RouteSplitPublishCoordinator<K, V> routeSplitPublishCoordinator;
    private final Executor splitExecutor;
    private final SplitFailureReporter failureReporter;
    private final SplitTelemetry telemetry;
    private final LongSupplier nanoTimeSupplier;
    private final Object splitMonitor = new Object();
    private final ConcurrentHashMap<SegmentId, Boolean> scheduledSplits = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<SegmentId, SplitAttemptState> splitAttemptStates = new ConcurrentHashMap<>();
    private final AtomicReference<RuntimeException> splitFailure = new AtomicReference<>();
    private final AtomicLong adaptiveSplitCooldownNanos = new AtomicLong(
            SPLIT_RESCHEDULE_COOLDOWN_NANOS);
    private int splitInFlightCount;

    SplitExecutionCoordinatorImpl(
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentTopology<K> segmentTopology,
            final RouteSplitCoordinator<K, V> routeSplitCoordinator,
            final RouteSplitPublishCoordinator<K, V> routeSplitPublishCoordinator,
            final Executor splitExecutor,
            final SplitFailureReporter failureReporter) {
        this(keyToSegmentMap, segmentTopology, routeSplitCoordinator,
                routeSplitPublishCoordinator, splitExecutor,
                failureReporter, SplitTelemetry.noOp(), System::nanoTime);
    }

    SplitExecutionCoordinatorImpl(
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentTopology<K> segmentTopology,
            final RouteSplitCoordinator<K, V> routeSplitCoordinator,
            final RouteSplitPublishCoordinator<K, V> routeSplitPublishCoordinator,
            final Executor splitExecutor,
            final SplitFailureReporter failureReporter,
            final SplitTelemetry telemetry) {
        this(keyToSegmentMap, segmentTopology, routeSplitCoordinator,
                routeSplitPublishCoordinator, splitExecutor,
                failureReporter, telemetry, System::nanoTime);
    }

    SplitExecutionCoordinatorImpl(final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentTopology<K> segmentTopology,
            final RouteSplitCoordinator<K, V> routeSplitCoordinator,
            final RouteSplitPublishCoordinator<K, V> routeSplitPublishCoordinator,
            final Executor splitExecutor,
            final SplitFailureReporter failureReporter,
            final LongSupplier nanoTimeSupplier) {
        this(keyToSegmentMap, segmentTopology, routeSplitCoordinator,
                routeSplitPublishCoordinator, splitExecutor,
                failureReporter, SplitTelemetry.noOp(), nanoTimeSupplier);
    }

    SplitExecutionCoordinatorImpl(final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentTopology<K> segmentTopology,
            final RouteSplitCoordinator<K, V> routeSplitCoordinator,
            final RouteSplitPublishCoordinator<K, V> routeSplitPublishCoordinator,
            final Executor splitExecutor,
            final SplitFailureReporter failureReporter,
            final SplitTelemetry telemetry,
            final LongSupplier nanoTimeSupplier) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentTopology = Vldtn.requireNonNull(segmentTopology,
                "segmentTopology");
        this.routeSplitCoordinator = Vldtn.requireNonNull(
                routeSplitCoordinator, "routeSplitCoordinator");
        this.routeSplitPublishCoordinator = Vldtn.requireNonNull(
                routeSplitPublishCoordinator,
                "routeSplitPublishCoordinator");
        this.splitExecutor = Vldtn.requireNonNull(splitExecutor,
                "splitExecutor");
        this.failureReporter = Vldtn.requireNonNull(failureReporter,
                "failureReporter");
        this.telemetry = Vldtn.requireNonNull(telemetry, "telemetry");
        this.nanoTimeSupplier = Vldtn.requireNonNull(nanoTimeSupplier,
                "nanoTimeSupplier");
    }

    @Override
    public boolean scheduleEligibleSplit(final SegmentHandle<K, V> segmentHandle,
            final long splitThreshold, final long observedKeyCount) {
        if (!hasCandidate(segmentHandle)) {
            return false;
        }
        final SegmentId segmentId = segmentHandle.getId();
        if (isClosedCandidate(segmentHandle)
                || isUnmappedCandidate(segmentId)
                || !canScheduleSplit(splitThreshold)) {
            clearAttemptState(segmentId);
            return false;
        }
        return scheduleAcceptedSplit(segmentHandle, splitThreshold,
                observedKeyCount);
    }

    @Override
    public void awaitSplitsIdle(final long timeoutMillis) {
        final RuntimeException failure = splitFailure.get();
        if (failure != null) {
            throw failure;
        }
        if (timeoutMillis <= 0L) {
            return;
        }
        final long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        synchronized (splitMonitor) {
            while (splitInFlightCount > 0) {
                final RuntimeException currentFailure = splitFailure.get();
                if (currentFailure != null) {
                    throw currentFailure;
                }
                final long remainingNanos = deadline - System.nanoTime();
                if (remainingNanos <= 0L) {
                    throw new IndexException(String.format(
                            "Split completion timed out after %d ms.",
                            timeoutMillis));
                }
                try {
                    TimeUnit.NANOSECONDS.timedWait(splitMonitor,
                            remainingNanos);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IndexException(
                            "Interrupted while waiting for split completion.",
                            e);
                }
            }
        }
        final RuntimeException completionFailure = splitFailure.get();
        if (completionFailure != null) {
            throw completionFailure;
        }
    }

    @Override
    public int splitInFlightCount() {
        synchronized (splitMonitor) {
            return splitInFlightCount;
        }
    }

    @Override
    public boolean isSplitBlocked(final SegmentId segmentId) {
        return segmentId != null && scheduledSplits.containsKey(segmentId);
    }

    @Override
    public int splitBlockedCount() {
        return scheduledSplits.size();
    }

    private boolean scheduleAcceptedSplit(
            final SegmentHandle<K, V> segmentHandle,
            final long splitThreshold, final long observedKeyCount) {
        final SegmentId segmentId = segmentHandle.getId();
        if (!isEligibleAfterCooldown(segmentId, observedKeyCount,
                splitThreshold)) {
            return false;
        }
        return scheduleSplitAsync(segmentHandle, splitThreshold,
                observedKeyCount);
    }

    private boolean scheduleSplitAsync(
            final SegmentHandle<K, V> segmentHandle,
            final long splitThreshold, final long observedKeyCount) {
        final SegmentId segmentId = segmentHandle.getId();
        if (!tryMarkSplitScheduled(segmentId)) {
            return false;
        }
        final long scheduledAtNanos = nanoTimeSupplier.getAsLong();
        try {
            splitExecutor.execute(
                    () -> executeScheduledSplit(segmentHandle, splitThreshold,
                            observedKeyCount, scheduledAtNanos));
        } catch (final RuntimeException e) {
            scheduledSplits.remove(segmentId);
            markSplitFinished();
            throw e;
        }
        return true;
    }

    private void executeScheduledSplit(
            final SegmentHandle<K, V> segmentHandle,
            final long splitThreshold, final long observedKeyCount,
            final long scheduledAtNanos) {
        final long startedAtNanos = nanoTimeSupplier.getAsLong();
        telemetry.recordSplitTaskStartDelayNanos(
                Math.max(0L, startedAtNanos - scheduledAtNanos));
        try {
            executeSplitAsync(segmentHandle, splitThreshold, observedKeyCount,
                    startedAtNanos);
        } finally {
            telemetry.recordSplitTaskRunLatencyNanos(Math.max(0L,
                    nanoTimeSupplier.getAsLong() - startedAtNanos));
        }
    }

    private void executeSplitAsync(final SegmentHandle<K, V> segmentHandle,
            final long splitThreshold, final long observedKeyCount,
            final long startNanos) {
        final SegmentId segmentId = segmentHandle.getId();
        boolean splitApplied = false;
        try {
            splitApplied = tryApplyPreparedSplit(segmentHandle, splitThreshold);
        } catch (final RuntimeException e) {
            splitFailure.compareAndSet(null, e);
            failureReporter.reportFailure(e);
        } finally {
            scheduledSplits.remove(segmentId);
            markSplitFinished();
        }
        final long durationNanos = Math.max(0L,
                nanoTimeSupplier.getAsLong() - startNanos);
        observeSplitDuration(durationNanos);
        if (splitApplied) {
            splitAttemptStates.remove(segmentId);
        } else {
            recordFailedSplitAttempt(segmentId, observedKeyCount);
        }
    }

    private boolean hasCandidate(final SegmentHandle<K, V> segmentHandle) {
        return segmentHandle != null;
    }

    private boolean isClosedCandidate(final SegmentHandle<K, V> segmentHandle) {
        return segmentHandle.getRuntime().getState() == SegmentState.CLOSED;
    }

    private boolean isUnmappedCandidate(final SegmentId segmentId) {
        return !keyToSegmentMap.getSegmentIds().contains(segmentId);
    }

    private void clearAttemptState(final SegmentId segmentId) {
        if (segmentId != null) {
            splitAttemptStates.remove(segmentId);
        }
    }

    private boolean canScheduleSplit(final long splitThreshold) {
        return isSplitSchedulingEnabled(splitThreshold);
    }

    private boolean tryApplyPreparedSplit(final SegmentHandle<K, V> segmentHandle,
            final long splitThreshold) {
        final RouteDrainResult drainResult = segmentTopology.tryBeginDrain(
                segmentHandle.getId());
        if (!drainResult.isAcquired()) {
            return false;
        }
        final RouteDrain drain = drainResult.drain();
        boolean published = false;
        try {
            drain.awaitDrained();
            final RouteSplitPlan<K> splitPlan = prepareSplit(segmentHandle,
                    splitThreshold);
            if (splitPlan == null) {
                return false;
            }
            published = publishPreparedSplit(splitPlan);
            return published;
        } finally {
            if (published) {
                completePublishedDrain(drain);
            } else if (isParentStillMapped(segmentHandle.getId())) {
                drain.abort();
            } else {
                completePublishedDrain(drain);
            }
        }
    }

    private void completePublishedDrain(final RouteDrain drain) {
        RuntimeException reconcileFailure = null;
        try {
            segmentTopology.reconcile(keyToSegmentMap.snapshot());
        } catch (final RuntimeException e) {
            reconcileFailure = e;
        } finally {
            drain.complete();
        }
        if (reconcileFailure != null) {
            throw reconcileFailure;
        }
    }

    private RouteSplitPlan<K> prepareSplit(
            final SegmentHandle<K, V> segmentHandle,
            final long splitThreshold) {
        return routeSplitCoordinator.tryPrepareSplit(segmentHandle,
                splitThreshold);
    }

    private boolean publishPreparedSplit(
            final RouteSplitPlan<K> splitPlan) {
        return routeSplitPublishCoordinator.applyPreparedSplit(splitPlan);
    }

    private boolean isParentStillMapped(final SegmentId segmentId) {
        return keyToSegmentMap.getSegmentIds().contains(segmentId);
    }

    private boolean tryMarkSplitScheduled(final SegmentId segmentId) {
        if (scheduledSplits.putIfAbsent(segmentId, Boolean.TRUE) != null) {
            return false;
        }
        markSplitStarted();
        return true;
    }

    private boolean isSplitSchedulingEnabled(final long splitThreshold) {
        return !Boolean.getBoolean("hestiastore.disableSplits")
                && splitThreshold >= 1L;
    }

    private boolean isEligibleAfterCooldown(final SegmentId segmentId,
            final long totalKeys, final long splitThreshold) {
        final SplitAttemptState state = splitAttemptStates.get(segmentId);
        if (state == null) {
            return true;
        }
        return state.shouldRetry(nanoTimeSupplier.getAsLong(), totalKeys,
                splitThreshold);
    }

    private void recordFailedSplitAttempt(final SegmentId segmentId,
            final long totalKeys) {
        splitAttemptStates.put(segmentId,
                new SplitAttemptState(nanoTimeSupplier.getAsLong(), totalKeys,
                        adaptiveSplitCooldownNanos.get()));
    }

    private void observeSplitDuration(final long durationNanos) {
        final long observedCooldown = clampCooldownNanos(
                multiplySaturated(durationNanos,
                        SPLIT_COOLDOWN_OBSERVED_MULTIPLIER));
        adaptiveSplitCooldownNanos.updateAndGet(previousCooldown -> clampCooldownNanos(
                (previousCooldown * SPLIT_COOLDOWN_SMOOTHING_WEIGHT_PREVIOUS
                        + observedCooldown
                                * SPLIT_COOLDOWN_SMOOTHING_WEIGHT_OBSERVED)
                        / (SPLIT_COOLDOWN_SMOOTHING_WEIGHT_PREVIOUS
                                + SPLIT_COOLDOWN_SMOOTHING_WEIGHT_OBSERVED)));
    }

    private static long clampCooldownNanos(final long candidateNanos) {
        return Math.max(SPLIT_RESCHEDULE_COOLDOWN_NANOS,
                Math.min(SPLIT_RESCHEDULE_COOLDOWN_MAX_NANOS,
                        candidateNanos));
    }

    private static long multiplySaturated(final long value,
            final long multiplier) {
        if (value <= 0L || multiplier <= 0L) {
            return 0L;
        }
        if (value > Long.MAX_VALUE / multiplier) {
            return Long.MAX_VALUE;
        }
        return value * multiplier;
    }

    private void markSplitStarted() {
        synchronized (splitMonitor) {
            splitInFlightCount++;
        }
    }

    private void markSplitFinished() {
        synchronized (splitMonitor) {
            if (splitInFlightCount > 0) {
                splitInFlightCount--;
            }
            splitMonitor.notifyAll();
        }
    }

    private static final class SplitAttemptState {
        private final long lastAttemptNanos;
        private final long lastObservedKeyCount;
        private final long cooldownNanos;

        private SplitAttemptState(final long lastAttemptNanos,
                final long lastObservedKeyCount,
                final long cooldownNanos) {
            this.lastAttemptNanos = lastAttemptNanos;
            this.lastObservedKeyCount = lastObservedKeyCount;
            this.cooldownNanos = cooldownNanos;
        }

        private boolean shouldRetry(final long nowNanos,
                final long currentKeyCount, final long splitThreshold) {
            if (currentKeyCount >= lastObservedKeyCount
                    + splitRetryGrowthThreshold(splitThreshold)) {
                return true;
            }
            return nowNanos - lastAttemptNanos >= cooldownNanos;
        }

        private static long splitRetryGrowthThreshold(
                final long splitThreshold) {
            final long byThreshold = Math.max(1L,
                    splitThreshold / SPLIT_RETRY_GROWTH_DIVISOR);
            return Math.min(SPLIT_RETRY_GROWTH_MAX_KEYS,
                    Math.max(SPLIT_RETRY_GROWTH_MIN_KEYS, byThreshold));
        }
    }
}
