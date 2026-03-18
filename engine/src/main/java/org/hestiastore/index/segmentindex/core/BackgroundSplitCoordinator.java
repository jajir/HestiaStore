package org.hestiastore.index.segmentindex.core;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.partition.PartitionRuntime;
import org.hestiastore.index.segmentindex.split.PartitionStableSplitCoordinator;

/**
 * Coordinates background split scheduling and split-apply admission.
 */
final class BackgroundSplitCoordinator<K, V> {

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

    private final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap;
    private final PartitionRuntime<K, V> partitionRuntime;
    private final PartitionStableSplitCoordinator<K, V> splitCoordinator;
    private final Executor splitExecutor;
    private final Consumer<RuntimeException> splitFailureHandler;
    private final Runnable splitAppliedListener;
    private final LongSupplier nanoTimeSupplier;
    private final Object splitMonitor = new Object();
    private final ReentrantReadWriteLock splitGate = new ReentrantReadWriteLock();
    private final ConcurrentHashMap<SegmentId, Boolean> scheduledSplits = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<SegmentId, SplitAttemptState> splitAttemptStates = new ConcurrentHashMap<>();
    private final AtomicReference<RuntimeException> splitFailure = new AtomicReference<>();
    private final AtomicInteger splitSchedulingPauseCount = new AtomicInteger();
    private final AtomicLong adaptiveSplitCooldownNanos = new AtomicLong(
            SPLIT_RESCHEDULE_COOLDOWN_NANOS);
    private int splitInFlightCount;

    BackgroundSplitCoordinator(
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final PartitionRuntime<K, V> partitionRuntime,
            final PartitionStableSplitCoordinator<K, V> splitCoordinator,
            final Executor splitExecutor,
            final Consumer<RuntimeException> splitFailureHandler,
            final Runnable splitAppliedListener) {
        this(keyToSegmentMap, partitionRuntime, splitCoordinator, splitExecutor,
                splitFailureHandler, splitAppliedListener, System::nanoTime);
    }

    BackgroundSplitCoordinator(
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final PartitionRuntime<K, V> partitionRuntime,
            final PartitionStableSplitCoordinator<K, V> splitCoordinator,
            final Executor splitExecutor,
            final Consumer<RuntimeException> splitFailureHandler,
            final Runnable splitAppliedListener,
            final LongSupplier nanoTimeSupplier) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.partitionRuntime = Vldtn.requireNonNull(partitionRuntime,
                "partitionRuntime");
        this.splitCoordinator = Vldtn.requireNonNull(splitCoordinator,
                "splitCoordinator");
        this.splitExecutor = Vldtn.requireNonNull(splitExecutor,
                "splitExecutor");
        this.splitFailureHandler = Vldtn.requireNonNull(splitFailureHandler,
                "splitFailureHandler");
        this.splitAppliedListener = Vldtn.requireNonNull(splitAppliedListener,
                "splitAppliedListener");
        this.nanoTimeSupplier = Vldtn.requireNonNull(nanoTimeSupplier,
                "nanoTimeSupplier");
    }

    boolean handleSplitCandidate(final Segment<K, V> segment,
            final long maxNumberOfKeysInPartitionBeforeSplit) {
        return handleSplitCandidate(segment,
                maxNumberOfKeysInPartitionBeforeSplit, false);
    }

    boolean forceHandleSplitCandidate(final Segment<K, V> segment,
            final long maxNumberOfKeysInPartitionBeforeSplit) {
        return handleSplitCandidate(segment,
                maxNumberOfKeysInPartitionBeforeSplit, true);
    }

    private boolean handleSplitCandidate(final Segment<K, V> segment,
            final long maxNumberOfKeysInPartitionBeforeSplit,
            final boolean ignoreCooldown) {
        if (segment == null) {
            return false;
        }
        final SegmentId segmentId = segment.getId();
        if (segment.getState() == SegmentState.CLOSED) {
            if (segmentId != null) {
                splitAttemptStates.remove(segmentId);
            }
            return false;
        }
        if (!isSplitSchedulingEnabled(maxNumberOfKeysInPartitionBeforeSplit)) {
            return false;
        }
        if (!keyToSegmentMap.getSegmentIds().contains(segmentId)) {
            if (segmentId != null) {
                splitAttemptStates.remove(segmentId);
            }
            return false;
        }
        return optionallyScheduleSplit(segment,
                maxNumberOfKeysInPartitionBeforeSplit, ignoreCooldown);
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

    <T> T runWithSplitSchedulingPaused(final Supplier<T> action) {
        Vldtn.requireNonNull(action, ACTION_PARAMETER);
        splitSchedulingPauseCount.incrementAndGet();
        try {
            return action.get();
        } finally {
            splitSchedulingPauseCount.decrementAndGet();
        }
    }

    void runWithSplitSchedulingPaused(final Runnable action) {
        Vldtn.requireNonNull(action, ACTION_PARAMETER);
        runWithSplitSchedulingPaused(() -> {
            action.run();
            return null;
        });
    }

    <T> T runWithStableWriteAdmission(final Supplier<T> action) {
        Vldtn.requireNonNull(action, ACTION_PARAMETER);
        final var readLock = splitGate.readLock();
        readLock.lock();
        try {
            return action.get();
        } finally {
            readLock.unlock();
        }
    }

    private boolean optionallyScheduleSplit(final Segment<K, V> segment,
            final long splitThreshold, final boolean ignoreCooldown) {
        if (!isSplitSchedulingEnabled(splitThreshold)
                || splitSchedulingPauseCount.get() > 0) {
            return false;
        }
        final SegmentId segmentId = segment.getId();
        final long totalKeys = segment.getNumberOfKeysInCache();
        if (totalKeys <= splitThreshold) {
            splitAttemptStates.remove(segmentId);
            return false;
        }
        if (!ignoreCooldown
                && !isEligibleAfterCooldown(segmentId, totalKeys,
                        splitThreshold)) {
            return false;
        }
        if (ignoreCooldown) {
            splitAttemptStates.remove(segmentId);
        }
        return scheduleSplitAsync(segment, splitThreshold, totalKeys);
    }

    private boolean scheduleSplitAsync(final Segment<K, V> segment,
            final long splitThreshold, final long observedKeyCount) {
        final SegmentId segmentId = segment.getId();
        if (scheduledSplits.putIfAbsent(segmentId, Boolean.TRUE) != null) {
            return false;
        }
        markSplitStarted();
        partitionRuntime.beginSplit(segmentId);
        try {
            splitExecutor.execute(
                    () -> executeSplitAsync(segment, splitThreshold,
                            observedKeyCount));
        } catch (final RuntimeException e) {
            scheduledSplits.remove(segmentId);
            partitionRuntime.finishSplit(segmentId);
            markSplitFinished();
            throw e;
        }
        return true;
    }

    private void executeSplitAsync(final Segment<K, V> segment,
            final long splitThreshold, final long observedKeyCount) {
        final SegmentId segmentId = segment.getId();
        final long startNanos = nanoTimeSupplier.getAsLong();
        boolean splitApplied = false;
        try {
            splitApplied = splitCoordinator.optionallySplit(segment,
                    splitThreshold,
                    this::runWithExclusiveSplitApply);
        } catch (final RuntimeException e) {
            splitFailure.compareAndSet(null, e);
            splitFailureHandler.accept(e);
        } finally {
            scheduledSplits.remove(segmentId);
            partitionRuntime.finishSplit(segmentId);
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

    private boolean runWithExclusiveSplitApply(final Supplier<Boolean> action) {
        Vldtn.requireNonNull(action, ACTION_PARAMETER);
        final var writeLock = splitGate.writeLock();
        writeLock.lock();
        try {
            return action.get();
        } finally {
            writeLock.unlock();
        }
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

    private static long splitRetryGrowthThreshold(final long splitThreshold) {
        final long byThreshold = Math.max(1L,
                splitThreshold / SPLIT_RETRY_GROWTH_DIVISOR);
        return Math.min(SPLIT_RETRY_GROWTH_MAX_KEYS,
                Math.max(SPLIT_RETRY_GROWTH_MIN_KEYS, byThreshold));
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
    }
}
