package org.hestiastore.index.segmentindex.core.split;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.Set;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.core.SegmentIndexRuntimeState;
import org.hestiastore.index.segmentindex.core.routing.MappedSegmentLeaseService;
import org.hestiastore.index.segmentindex.core.routing.RouteSplitLease;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.hestiastore.index.segmentindex.routemap.RouteSplitPlan;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of split scheduling and split-publish admission.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SplitTaskCoordinator<K, V> {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(SplitTaskCoordinator.class);
    private static final long SPLIT_RESCHEDULE_COOLDOWN_MILLIS = 500L;
    private static final long SPLIT_RESCHEDULE_COOLDOWN_NANOS = TimeUnit.MILLISECONDS
            .toNanos(SPLIT_RESCHEDULE_COOLDOWN_MILLIS);
    private static final long SPLIT_RESCHEDULE_COOLDOWN_MAX_MILLIS = 5_000L;
    private static final long SPLIT_RESCHEDULE_COOLDOWN_MAX_NANOS = TimeUnit.MILLISECONDS
            .toNanos(SPLIT_RESCHEDULE_COOLDOWN_MAX_MILLIS);
    private static final long SPLIT_COOLDOWN_OBSERVED_MULTIPLIER = 2L;
    private static final long SPLIT_COOLDOWN_SMOOTHING_WEIGHT_PREVIOUS = 3L;
    private static final long SPLIT_COOLDOWN_SMOOTHING_WEIGHT_OBSERVED = 1L;

    private final SegmentRouteMap<K> keyToSegmentMap;
    private final MappedSegmentLeaseService<K, V> segmentLeaseService;
    private final RouteSplitPlanner<K, V> routeSplitCoordinator;
    private final RouteSplitPublisher<K, V> routeSplitPublishCoordinator;
    private final Executor splitExecutor;
    private final SegmentIndexRuntimeState runtimeState;
    private final SplitStatsRecorder statsRecorder;
    private final LongSupplier nanoTimeSupplier;
    private final Object splitMonitor = new Object();
    private final Set<SegmentId> scheduledSplits = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<SegmentId, SplitRetryState> splitAttemptStates = new ConcurrentHashMap<>();
    private final AtomicReference<RuntimeException> splitFailure = new AtomicReference<>();
    private final AtomicLong adaptiveSplitCooldownNanos = new AtomicLong(
            SPLIT_RESCHEDULE_COOLDOWN_NANOS);
    private int splitInFlightCount;

    SplitTaskCoordinator(final SegmentRouteMap<K> keyToSegmentMap,
            final MappedSegmentLeaseService<K, V> segmentLeaseService,
            final RouteSplitPlanner<K, V> routeSplitCoordinator,
            final RouteSplitPublisher<K, V> routeSplitPublishCoordinator,
            final Executor splitExecutor,
            final SegmentIndexRuntimeState runtimeState,
            final SplitStatsRecorder statsRecorder,
            final LongSupplier nanoTimeSupplier) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentLeaseService = Vldtn.requireNonNull(segmentLeaseService,
                "segmentLeaseService");
        this.routeSplitCoordinator = Vldtn.requireNonNull(
                routeSplitCoordinator, "routeSplitCoordinator");
        this.routeSplitPublishCoordinator = Vldtn.requireNonNull(
                routeSplitPublishCoordinator,
                "routeSplitPublishCoordinator");
        this.splitExecutor = Vldtn.requireNonNull(splitExecutor,
                "splitExecutor");
        this.runtimeState = Vldtn.requireNonNull(runtimeState, "runtimeState");
        this.statsRecorder = Vldtn.requireNonNull(statsRecorder,
                "statsRecorder");
        this.nanoTimeSupplier = Vldtn.requireNonNull(nanoTimeSupplier,
                "nanoTimeSupplier");
    }

    boolean scheduleEligibleSplit(final SegmentId segmentId,
            final long splitThreshold, final long observedKeyCount) {
        if (segmentId == null) {
            return false;
        }
        if (!isSplitSchedulingEnabled(splitThreshold)) {
            clearAttemptState(segmentId);
            return false;
        }
        return scheduleAcceptedSplit(segmentId, splitThreshold,
                observedKeyCount);
    }

    void awaitSplitsIdle(final long timeoutMillis) {
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

    int splitInFlightCount() {
        synchronized (splitMonitor) {
            return splitInFlightCount;
        }
    }

    boolean isSplitBlocked(final SegmentId segmentId) {
        return segmentId != null && scheduledSplits.contains(segmentId);
    }

    int splitBlockedCount() {
        return scheduledSplits.size();
    }

    private boolean scheduleAcceptedSplit(
            final SegmentId segmentId,
            final long splitThreshold, final long observedKeyCount) {
        if (!isEligibleAfterCooldown(segmentId, observedKeyCount,
                splitThreshold)) {
            return false;
        }
        return scheduleSplitAsync(segmentId, splitThreshold,
                observedKeyCount);
    }

    private boolean scheduleSplitAsync(
            final SegmentId segmentId,
            final long splitThreshold, final long observedKeyCount) {
        if (!tryMarkSplitScheduled(segmentId)) {
            return false;
        }
        final long scheduledAtNanos = nanoTimeSupplier.getAsLong();
        try {
            splitExecutor.execute(
                    () -> executeScheduledSplit(segmentId, splitThreshold,
                            observedKeyCount, scheduledAtNanos));
        } catch (final RuntimeException e) {
            scheduledSplits.remove(segmentId);
            markSplitFinished();
            throw e;
        }
        return true;
    }

    private void executeScheduledSplit(
            final SegmentId segmentId,
            final long splitThreshold, final long observedKeyCount,
            final long scheduledAtNanos) {
        final long startedAtNanos = nanoTimeSupplier.getAsLong();
        statsRecorder.recordSplitTaskStartDelayNanos(
                Math.max(0L, startedAtNanos - scheduledAtNanos));
        try {
            executeSplitAsync(segmentId, splitThreshold, observedKeyCount,
                    startedAtNanos);
        } finally {
            statsRecorder.recordSplitTaskRunLatencyNanos(Math.max(0L,
                    nanoTimeSupplier.getAsLong() - startedAtNanos));
        }
    }

    private void executeSplitAsync(final SegmentId segmentId,
            final long splitThreshold, final long observedKeyCount,
            final long startNanos) {
        boolean splitApplied = false;
        RuntimeException failure = null;
        try {
            splitApplied = tryApplyPreparedSplit(segmentId, splitThreshold,
                    observedKeyCount);
        } catch (final RuntimeException e) {
            failure = e;
            splitFailure.compareAndSet(null, e);
            runtimeState.markRuntimeFailure(e);
        } finally {
            scheduledSplits.remove(segmentId);
            markSplitFinished();
        }
        final long durationNanos = Math.max(0L,
                nanoTimeSupplier.getAsLong() - startNanos);
        if (failure == null) {
            logSplitFinished(segmentId, splitThreshold, observedKeyCount,
                    splitApplied, durationNanos);
        } else {
            logSplitFailed(segmentId, splitThreshold, observedKeyCount,
                    durationNanos, failure);
        }
        observeSplitDuration(durationNanos);
        if (splitApplied) {
            splitAttemptStates.remove(segmentId);
        } else {
            recordFailedSplitAttempt(segmentId, observedKeyCount);
        }
    }

    private boolean isClosedCandidate(final BlockingSegment<K, V> segmentHandle) {
        return segmentHandle.getRuntime().getState() == SegmentState.CLOSED;
    }

    private void clearAttemptState(final SegmentId segmentId) {
        if (segmentId != null) {
            splitAttemptStates.remove(segmentId);
        }
    }

    private boolean tryApplyPreparedSplit(final SegmentId segmentId,
            final long splitThreshold, final long observedKeyCount) {
        final Optional<RouteSplitLease<K, V>> splitLease =
                segmentLeaseService.tryAcquireForSplit(segmentId);
        if (splitLease.isEmpty()) {
            logSplitSkippedBecauseDrainUnavailable(segmentId, splitThreshold);
            return false;
        }
        try (RouteSplitLease<K, V> lease = splitLease.get()) {
            if (isClosedCandidate(lease.segment())) {
                logSplitAbortedBecauseSegmentClosed(segmentId);
                return false;
            }
            final RouteSplitPreparation<K> routeSplit = routeSplitCoordinator
                    .tryPrepareSplit(lease.segment(), splitThreshold,
                            observedKeyCount);
            return applyPreparedSplit(routeSplit, lease);
        }
    }

    private boolean applyPreparedSplit(
            final RouteSplitPreparation<K> routeSplit,
            final RouteSplitLease<K, V> lease) {
        if (routeSplit.status() == RouteSplitPreparationStatus.PREPARED) {
            return publishSplit(routeSplit.routeSplit().orElseThrow(), lease);
        }
        if (routeSplit.status() == RouteSplitPreparationStatus.COMPACT_PARENT) {
            compactParentSegment(lease);
        }
        return false;
    }

    private void compactParentSegment(final RouteSplitLease<K, V> lease) {
        try {
            lease.segment().compact();
        } finally {
            lease.abort();
        }
    }

    private boolean publishSplit(final RouteSplitPlan<K> routeSplit,
            final RouteSplitLease<K, V> splitLease) {
        boolean published = false;
        try {
            published = routeSplitPublishCoordinator.applyPreparedSplit(
                    routeSplit);
            return published;
        } finally {
            if (published || !isParentStillMapped(splitLease.segmentId())) {
                splitLease.completeAfterPublish();
            } else {
                splitLease.abort();
            }
        }
    }

    private boolean isParentStillMapped(final SegmentId segmentId) {
        return keyToSegmentMap.getSegmentIds().contains(segmentId);
    }

    private boolean tryMarkSplitScheduled(final SegmentId segmentId) {
        if (!scheduledSplits.add(segmentId)) {
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
        final SplitRetryState state = splitAttemptStates.get(segmentId);
        if (state == null) {
            return true;
        }
        return state.shouldRetry(nanoTimeSupplier.getAsLong(), totalKeys,
                splitThreshold);
    }

    private void recordFailedSplitAttempt(final SegmentId segmentId,
            final long totalKeys) {
        splitAttemptStates.put(segmentId,
                new SplitRetryState(nanoTimeSupplier.getAsLong(), totalKeys,
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

    private void logSplitSkippedBecauseDrainUnavailable(
            final SegmentId segmentId, final long splitThreshold) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    "Route split skipped because route drain could not be acquired: segment='{}' threshold='{}'",
                    segmentId, splitThreshold);
        }
    }

    private void logSplitAbortedBecauseSegmentClosed(
            final SegmentId segmentId) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    "Route split aborted because parent segment was already closed after route drain: segment='{}'",
                    segmentId);
        }
    }

    private void logSplitFinished(final SegmentId segmentId,
            final long splitThreshold, final long observedKeyCount,
            final boolean splitApplied, final long durationNanos) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    "Route split finished: segment='{}' threshold='{}' observedKeyCount='{}' outcome='{}' durationMillis='{}'",
                    segmentId, splitThreshold, observedKeyCount,
                    splitApplied ? "published" : "not-published",
                    nanosToMillis(durationNanos));
        }
    }

    private void logSplitFailed(final SegmentId segmentId,
            final long splitThreshold, final long observedKeyCount,
            final long durationNanos, final RuntimeException failure) {
        LOGGER.warn(
                "Route split failed: segment='{}' threshold='{}' observedKeyCount='{}' durationMillis='{}'",
                segmentId, splitThreshold, observedKeyCount,
                nanosToMillis(durationNanos), failure);
    }

    private static long nanosToMillis(final long nanos) {
        return TimeUnit.NANOSECONDS.toMillis(Math.max(0L, nanos));
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

}
