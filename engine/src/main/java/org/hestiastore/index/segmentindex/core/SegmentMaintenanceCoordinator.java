package org.hestiastore.index.segmentindex.core;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.split.SegmentSplitCoordinator;

/**
 * Coordinates explicit split scheduling after partition drain or maintenance
 * boundaries.
 */
final class SegmentMaintenanceCoordinator<K, V> {

    private final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap;
    private final SegmentSplitCoordinator<K, V> splitCoordinator;
    private final Object splitMonitor = new Object();
    private final ReentrantReadWriteLock splitGate = new ReentrantReadWriteLock();
    private int splitInFlightCount;

    SegmentMaintenanceCoordinator(
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final SegmentSplitCoordinator<K, V> splitCoordinator) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.splitCoordinator = Vldtn.requireNonNull(splitCoordinator,
                "splitCoordinator");
    }

    void handlePostDrain(final Segment<K, V> segment,
            final long maxNumberOfKeysInPartitionBeforeSplit) {
        if (segment == null || segment.getState() == SegmentState.CLOSED) {
            return;
        }
        if (!isSplitSchedulingEnabled(maxNumberOfKeysInPartitionBeforeSplit)) {
            return;
        }
        if (!keyToSegmentMap.getSegmentIds().contains(segment.getId())) {
            return;
        }
        optionallyScheduleSplit(segment, maxNumberOfKeysInPartitionBeforeSplit);
    }

    void awaitSplitsIdle(final long timeoutMillis) {
        if (timeoutMillis <= 0L) {
            return;
        }
        final long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        synchronized (splitMonitor) {
            while (splitInFlightCount > 0) {
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
    }

    int splitInFlightCount() {
        synchronized (splitMonitor) {
            return splitInFlightCount;
        }
    }

    <T> T runWithStableWriteAdmission(final Supplier<T> action) {
        Vldtn.requireNonNull(action, "action");
        final var readLock = splitGate.readLock();
        readLock.lock();
        try {
            return action.get();
        } finally {
            readLock.unlock();
        }
    }

    void runWithExclusiveWriteAdmission(final Runnable action) {
        Vldtn.requireNonNull(action, "action");
        final var writeLock = splitGate.writeLock();
        writeLock.lock();
        try {
            action.run();
        } finally {
            writeLock.unlock();
        }
    }

    private void optionallyScheduleSplit(final Segment<K, V> segment,
            final long splitThreshold) {
        if (!isSplitSchedulingEnabled(splitThreshold)) {
            return;
        }
        final long totalKeys = segment.getNumberOfKeysInCache();
        if (totalKeys > splitThreshold) {
            markSplitStarted();
            final var writeLock = splitGate.writeLock();
            writeLock.lock();
            try {
                splitCoordinator.optionallySplit(segment, splitThreshold);
            } finally {
                writeLock.unlock();
                markSplitFinished();
            }
        }
    }

    private boolean isSplitSchedulingEnabled(final long splitThreshold) {
        return !Boolean.getBoolean("hestiastore.disableSplits")
                && splitThreshold >= 1L;
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
