package org.hestiastore.index.segmentindex.core.split;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.core.segmentlease.SegmentLeaseService;
import org.hestiastore.index.segmentindex.core.segmentlease.SegmentSplitLease;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.SegmentRouteSplit;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of split scheduling and split-publish admission.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SplitExecutionCoordinator<K, V> {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(SplitExecutionCoordinator.class);
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
    private final SegmentLeaseService<K, V> segmentLeaseService;
    private final RouteSplitCoordinator<K, V> routeSplitCoordinator;
    private final RouteSplitPublishCoordinator<K, V> routeSplitPublishCoordinator;
    private final Executor splitExecutor;
    private final Consumer<RuntimeException> failureReporter;
    private final SplitStatsRecorder statsRecorder;
    private final LongSupplier nanoTimeSupplier;
    private final Object splitMonitor = new Object();
    private final ConcurrentHashMap<SegmentId, Boolean> scheduledSplits = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<SegmentId, SplitAttemptState> splitAttemptStates = new ConcurrentHashMap<>();
    private final AtomicReference<RuntimeException> splitFailure = new AtomicReference<>();
    private final AtomicLong adaptiveSplitCooldownNanos = new AtomicLong(
            SPLIT_RESCHEDULE_COOLDOWN_NANOS);
    private int splitInFlightCount;

    SplitExecutionCoordinator(final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentLeaseService<K, V> segmentLeaseService,
            final RouteSplitCoordinator<K, V> routeSplitCoordinator,
            final RouteSplitPublishCoordinator<K, V> routeSplitPublishCoordinator,
            final Executor splitExecutor,
            final Consumer<RuntimeException> failureReporter,
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
        this.failureReporter = Vldtn.requireNonNull(failureReporter,
                "failureReporter");
        this.statsRecorder = Vldtn.requireNonNull(statsRecorder,
                "statsRecorder");
        this.nanoTimeSupplier = Vldtn.requireNonNull(nanoTimeSupplier,
                "nanoTimeSupplier");
    }

    public boolean scheduleEligibleSplit(final SegmentId segmentId,
            final long splitThreshold, final long observedKeyCount) {
        if (!hasCandidate(segmentId)) {
            return false;
        }
        if (!canScheduleSplit(splitThreshold)) {
            clearAttemptState(segmentId);
            return false;
        }
        return scheduleAcceptedSplit(segmentId, splitThreshold,
                observedKeyCount);
    }

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

    public int splitInFlightCount() {
        synchronized (splitMonitor) {
            return splitInFlightCount;
        }
    }

    public boolean isSplitBlocked(final SegmentId segmentId) {
        return segmentId != null && scheduledSplits.containsKey(segmentId);
    }

    public int splitBlockedCount() {
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
            splitApplied = tryApplyPreparedSplit(segmentId, splitThreshold);
        } catch (final RuntimeException e) {
            failure = e;
            splitFailure.compareAndSet(null, e);
            failureReporter.accept(e);
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

    private boolean hasCandidate(final SegmentId segmentId) {
        return segmentId != null;
    }

    private boolean isClosedCandidate(final BlockingSegment<K, V> segmentHandle) {
        return segmentHandle.getRuntime().getState() == SegmentState.CLOSED;
    }

    private void clearAttemptState(final SegmentId segmentId) {
        if (segmentId != null) {
            splitAttemptStates.remove(segmentId);
        }
    }

    private boolean canScheduleSplit(final long splitThreshold) {
        return isSplitSchedulingEnabled(splitThreshold);
    }

    private boolean tryApplyPreparedSplit(final SegmentId segmentId,
            final long splitThreshold) {
        final Optional<SegmentSplitLease<K, V>> splitLease =
                segmentLeaseService.tryAcquireForSplit(segmentId);
        if (splitLease.isEmpty()) {
            logSplitSkippedBecauseDrainUnavailable(segmentId, splitThreshold);
            return false;
        }
        try (SegmentSplitLease<K, V> lease = splitLease.get()) {
            if (isClosedCandidate(lease.segment())) {
                logSplitAbortedBecauseSegmentClosed(segmentId);
                return false;
            }
            final SegmentRouteSplit<K> routeSplit = prepareSplit(
                    lease.segment(), splitThreshold);
            if (routeSplit == null) {
                return false;
            }
            return publishSplit(routeSplit, lease);
        }
    }

    private SegmentRouteSplit<K> prepareSplit(
            final BlockingSegment<K, V> segmentHandle,
            final long splitThreshold) {
        return routeSplitCoordinator.tryPrepareSplit(segmentHandle,
                splitThreshold);
    }

    private boolean publishPreparedSplit(
            final SegmentRouteSplit<K> routeSplit) {
        return routeSplitPublishCoordinator.applyPreparedSplit(routeSplit);
    }

    private boolean publishSplit(final SegmentRouteSplit<K> routeSplit,
            final SegmentSplitLease<K, V> splitLease) {
        boolean published = false;
        try {
            published = publishPreparedSplit(routeSplit);
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
