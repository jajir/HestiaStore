package org.hestiastore.index.segmentindex.core.routing;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentHandle;

/**
 * Default implementation of background split scheduling and split-publish
 * admission.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class BackgroundSplitCoordinatorImpl<K, V>
        implements BackgroundSplitCoordinator<K, V> {

    private static final String ACTION_PARAMETER = "action";
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
    private final RouteSplitCoordinator<K, V> routeSplitCoordinator;
    private final RouteSplitPublishCoordinator<K, V> routeSplitPublishCoordinator;
    private final Executor splitExecutor;
    private final Consumer<RuntimeException> splitFailureHandler;
    private final Runnable splitAppliedListener;
    private final LongConsumer splitTaskStartDelayRecorder;
    private final LongConsumer splitTaskRunLatencyRecorder;
    private final LongSupplier nanoTimeSupplier;
    private final Object splitMonitor = new Object();
    // Fair ordering prevents retrying direct writes from starving split publish.
    private final ReentrantReadWriteLock splitGate = new ReentrantReadWriteLock(
            true);
    private final ConcurrentHashMap<SegmentId, Boolean> scheduledSplits = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<SegmentId, SplitAttemptState> splitAttemptStates = new ConcurrentHashMap<>();
    private final AtomicReference<RuntimeException> splitFailure = new AtomicReference<>();
    private final AtomicInteger splitSchedulingPauseCount = new AtomicInteger();
    private final AtomicLong adaptiveSplitCooldownNanos = new AtomicLong(
            SPLIT_RESCHEDULE_COOLDOWN_NANOS);
    private int splitInFlightCount;

    BackgroundSplitCoordinatorImpl(
            final KeyToSegmentMap<K> keyToSegmentMap,
            final RouteSplitCoordinator<K, V> routeSplitCoordinator,
            final RouteSplitPublishCoordinator<K, V> routeSplitPublishCoordinator,
            final Executor splitExecutor,
            final Consumer<RuntimeException> splitFailureHandler,
            final Runnable splitAppliedListener) {
        this(keyToSegmentMap, routeSplitCoordinator,
                routeSplitPublishCoordinator, splitExecutor,
                splitFailureHandler, splitAppliedListener,
                ignored -> { }, ignored -> { }, System::nanoTime);
    }

    BackgroundSplitCoordinatorImpl(
            final KeyToSegmentMap<K> keyToSegmentMap,
            final RouteSplitCoordinator<K, V> routeSplitCoordinator,
            final RouteSplitPublishCoordinator<K, V> routeSplitPublishCoordinator,
            final Executor splitExecutor,
            final Consumer<RuntimeException> splitFailureHandler,
            final Runnable splitAppliedListener,
            final LongConsumer splitTaskStartDelayRecorder,
            final LongConsumer splitTaskRunLatencyRecorder) {
        this(keyToSegmentMap, routeSplitCoordinator,
                routeSplitPublishCoordinator, splitExecutor,
                splitFailureHandler, splitAppliedListener,
                splitTaskStartDelayRecorder, splitTaskRunLatencyRecorder,
                System::nanoTime);
    }

    BackgroundSplitCoordinatorImpl(final KeyToSegmentMap<K> keyToSegmentMap,
            final RouteSplitCoordinator<K, V> routeSplitCoordinator,
            final RouteSplitPublishCoordinator<K, V> routeSplitPublishCoordinator,
            final Executor splitExecutor,
            final Consumer<RuntimeException> splitFailureHandler,
            final Runnable splitAppliedListener,
            final LongSupplier nanoTimeSupplier) {
        this(keyToSegmentMap, routeSplitCoordinator,
                routeSplitPublishCoordinator, splitExecutor,
                splitFailureHandler, splitAppliedListener,
                ignored -> { }, ignored -> { }, nanoTimeSupplier);
    }

    BackgroundSplitCoordinatorImpl(final KeyToSegmentMap<K> keyToSegmentMap,
            final RouteSplitCoordinator<K, V> routeSplitCoordinator,
            final RouteSplitPublishCoordinator<K, V> routeSplitPublishCoordinator,
            final Executor splitExecutor,
            final Consumer<RuntimeException> splitFailureHandler,
            final Runnable splitAppliedListener,
            final LongConsumer splitTaskStartDelayRecorder,
            final LongConsumer splitTaskRunLatencyRecorder,
            final LongSupplier nanoTimeSupplier) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.routeSplitCoordinator = Vldtn.requireNonNull(
                routeSplitCoordinator, "routeSplitCoordinator");
        this.routeSplitPublishCoordinator = Vldtn.requireNonNull(
                routeSplitPublishCoordinator,
                "routeSplitPublishCoordinator");
        this.splitExecutor = Vldtn.requireNonNull(splitExecutor,
                "splitExecutor");
        this.splitFailureHandler = Vldtn.requireNonNull(splitFailureHandler,
                "splitFailureHandler");
        this.splitAppliedListener = Vldtn.requireNonNull(splitAppliedListener,
                "splitAppliedListener");
        this.splitTaskStartDelayRecorder = Vldtn.requireNonNull(
                splitTaskStartDelayRecorder, "splitTaskStartDelayRecorder");
        this.splitTaskRunLatencyRecorder = Vldtn.requireNonNull(
                splitTaskRunLatencyRecorder, "splitTaskRunLatencyRecorder");
        this.nanoTimeSupplier = Vldtn.requireNonNull(nanoTimeSupplier,
                "nanoTimeSupplier");
    }

    @Override
    public boolean handleSplitCandidate(
            final SegmentHandle<K, V> segmentHandle,
            final long splitThreshold, final boolean ignoreCooldown) {
        if (!hasCandidate(segmentHandle)) {
            return false;
        }
        final SegmentId segmentId = segmentHandle.getId();
        if (isClosedCandidate(segmentHandle)
                || isUnmappedCandidate(segmentId)
                || !isSplitSchedulingEnabled(splitThreshold)) {
            clearAttemptState(segmentId);
            return false;
        }
        return optionallyScheduleSplit(segmentHandle, splitThreshold,
                ignoreCooldown);
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

    @Override
    public <T> T runWithSplitSchedulingPaused(final Supplier<T> action) {
        Vldtn.requireNonNull(action, ACTION_PARAMETER);
        splitSchedulingPauseCount.incrementAndGet();
        try {
            return action.get();
        } finally {
            splitSchedulingPauseCount.decrementAndGet();
        }
    }

    @Override
    public void runWithSplitSchedulingPaused(final Runnable action) {
        Vldtn.requireNonNull(action, ACTION_PARAMETER);
        runWithSplitSchedulingPaused(() -> {
            action.run();
            return null;
        });
    }

    @Override
    public <T> T runWithSharedSplitAdmission(final Supplier<T> action) {
        Vldtn.requireNonNull(action, ACTION_PARAMETER);
        final var readLock = splitGate.readLock();
        readLock.lock();
        try {
            return action.get();
        } finally {
            readLock.unlock();
        }
    }

    private boolean optionallyScheduleSplit(
            final SegmentHandle<K, V> segmentHandle,
            final long splitThreshold, final boolean ignoreCooldown) {
        if (!canScheduleSplit(splitThreshold)) {
            return false;
        }
        final SegmentId segmentId = segmentHandle.getId();
        final long totalKeys = observedSegmentKeyCount(segmentHandle);
        if (!exceedsSplitThreshold(totalKeys, splitThreshold)) {
            clearAttemptState(segmentId);
            return false;
        }
        if (!ignoreCooldown
                && !isEligibleAfterCooldown(segmentId, totalKeys,
                        splitThreshold)) {
            return false;
        }
        if (ignoreCooldown) {
            clearAttemptState(segmentId);
        }
        return scheduleSplitAsync(segmentHandle, splitThreshold, totalKeys);
    }

    private boolean scheduleSplitAsync(
            final SegmentHandle<K, V> segmentHandle,
            final long splitThreshold, final long observedKeyCount) {
        final SegmentId segmentId = segmentHandle.getId();
        if (!runWithExclusiveSplitAdmission(() -> tryMarkSplitScheduled(
                segmentId))) {
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
        splitTaskStartDelayRecorder.accept(
                Math.max(0L, startedAtNanos - scheduledAtNanos));
        try {
            executeSplitAsync(segmentHandle, splitThreshold, observedKeyCount,
                    startedAtNanos);
        } finally {
            splitTaskRunLatencyRecorder.accept(Math.max(0L,
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
            splitFailureHandler.accept(e);
        } finally {
            scheduledSplits.remove(segmentId);
            markSplitFinished();
        }
        final long durationNanos = Math.max(0L,
                nanoTimeSupplier.getAsLong() - startNanos);
        observeSplitDuration(durationNanos);
        if (splitApplied) {
            splitAttemptStates.remove(segmentId);
            notifySplitApplied();
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
        return isSplitSchedulingEnabled(splitThreshold)
                && splitSchedulingPauseCount.get() == 0;
    }

    private long observedSegmentKeyCount(final SegmentHandle<K, V> segmentHandle) {
        return segmentHandle.getRuntime().getNumberOfKeysInCache();
    }

    private boolean exceedsSplitThreshold(final long totalKeys,
            final long splitThreshold) {
        return totalKeys > splitThreshold;
    }

    private boolean tryApplyPreparedSplit(final SegmentHandle<K, V> segmentHandle,
            final long splitThreshold) {
        final RouteSplitPlan<K> splitPlan = prepareSplit(segmentHandle,
                splitThreshold);
        if (splitPlan == null) {
            return false;
        }
        return publishPreparedSplit(splitPlan);
    }

    private RouteSplitPlan<K> prepareSplit(
            final SegmentHandle<K, V> segmentHandle,
            final long splitThreshold) {
        return routeSplitCoordinator.tryPrepareSplit(segmentHandle,
                splitThreshold);
    }

    private boolean publishPreparedSplit(
            final RouteSplitPlan<K> splitPlan) {
        return runWithExclusiveSplitAdmission(
                () -> routeSplitPublishCoordinator
                        .applyPreparedSplit(splitPlan));
    }

    private boolean runWithExclusiveSplitAdmission(
            final BooleanSupplier action) {
        Vldtn.requireNonNull(action, ACTION_PARAMETER);
        final var writeLock = splitGate.writeLock();
        writeLock.lock();
        try {
            return action.getAsBoolean();
        } finally {
            writeLock.unlock();
        }
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

    private void notifySplitApplied() {
        try {
            splitAppliedListener.run();
        } catch (final RuntimeException e) {
            splitFailure.compareAndSet(null, e);
            splitFailureHandler.accept(e);
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
