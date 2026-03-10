package org.hestiastore.index.segmentindex.partition;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.hestiastore.index.Vldtn;

/**
 * Buffered write state for a single routed partition.
 *
 * @param <K> key type
 * @param <V> value type
 * @author honza
 */
final class RangePartition<K, V> {

    private final Comparator<K> keyComparator;
    private final Object monitor = new Object();
    private final Deque<PartitionImmutableRun<K, V>> immutableRuns = new ArrayDeque<>();
    private TreeMap<K, V> activeEntries;
    private int bufferedKeyCount;
    private boolean drainScheduled;
    private int splitBlockCount;

    RangePartition(final Comparator<K> keyComparator) {
        this.keyComparator = Vldtn.requireNonNull(keyComparator,
                "keyComparator");
        this.activeEntries = new TreeMap<>(keyComparator);
    }

    PartitionWriteResult write(final K key, final V value,
            final PartitionRuntimeLimits limits,
            final AtomicInteger totalBufferedKeyCount,
            final AtomicInteger localThrottleCount,
            final AtomicInteger globalThrottleCount) {
        Vldtn.requireNonNull(key, "key");
        Vldtn.requireNonNull(value, "value");
        Vldtn.requireNonNull(limits, "limits");
        synchronized (monitor) {
            final boolean existingActive = activeEntries.containsKey(key);
            if (!existingActive && exceedsLocalCapacity(limits)) {
                localThrottleCount.incrementAndGet();
                return PartitionWriteResult.busy();
            }
            if (!existingActive
                    && !reserveGlobal(totalBufferedKeyCount,
                            limits.getMaxNumberOfKeysInIndexBuffer())) {
                globalThrottleCount.incrementAndGet();
                return PartitionWriteResult.busy();
            }
            activeEntries.put(key, value);
            if (!existingActive) {
                bufferedKeyCount++;
            }
            final boolean canRotate = activeEntries
                    .size() >= limits.getMaxNumberOfKeysInActivePartition()
                    && immutableRuns.size() < limits
                            .getMaxNumberOfImmutableRunsPerPartition();
            if (canRotate) {
                rotateActiveUnsafe();
            }
            return PartitionWriteResult.ok(!immutableRuns.isEmpty());
        }
    }

    PartitionLookupResult<V> lookup(final K key) {
        Vldtn.requireNonNull(key, "key");
        synchronized (monitor) {
            if (activeEntries.containsKey(key)) {
                return PartitionLookupResult.hit(activeEntries.get(key));
            }
            final List<PartitionImmutableRun<K, V>> reversed = new ArrayList<>(
                    immutableRuns);
            for (int i = reversed.size() - 1; i >= 0; i--) {
                final PartitionImmutableRun<K, V> run = reversed.get(i);
                if (run.getEntries().containsKey(key)) {
                    return PartitionLookupResult
                            .hit(run.getEntries().get(key));
                }
            }
            return PartitionLookupResult.miss();
        }
    }

    void sealActiveForDrain() {
        synchronized (monitor) {
            if (activeEntries.isEmpty()) {
                return;
            }
            rotateActiveUnsafe();
        }
    }

    boolean markDrainScheduledIfNeeded() {
        synchronized (monitor) {
            if (splitBlockCount > 0 || drainScheduled
                    || immutableRuns.isEmpty()) {
                return false;
            }
            drainScheduled = true;
            return true;
        }
    }

    PartitionImmutableRun<K, V> peekOldestImmutableRun() {
        synchronized (monitor) {
            return immutableRuns.peekFirst();
        }
    }

    int completeDrainedRun(final PartitionImmutableRun<K, V> run,
            final AtomicInteger totalBufferedKeyCount) {
        Vldtn.requireNonNull(run, "run");
        synchronized (monitor) {
            final PartitionImmutableRun<K, V> current = immutableRuns.peekFirst();
            if (current != run) {
                return 0;
            }
            immutableRuns.removeFirst();
            bufferedKeyCount -= run.size();
            if (bufferedKeyCount < 0) {
                bufferedKeyCount = 0;
            }
            totalBufferedKeyCount.addAndGet(-run.size());
            return run.size();
        }
    }

    void finishDrainScheduling() {
        synchronized (monitor) {
            drainScheduled = false;
        }
    }

    void applyOverlaySnapshot(final NavigableMap<K, V> target) {
        synchronized (monitor) {
            for (final PartitionImmutableRun<K, V> run : immutableRuns) {
                target.putAll(run.getEntries());
            }
            target.putAll(activeEntries);
        }
    }

    int getBufferedKeyCount() {
        synchronized (monitor) {
            return bufferedKeyCount;
        }
    }

    int getImmutableRunCount() {
        synchronized (monitor) {
            return immutableRuns.size();
        }
    }

    boolean hasActiveEntries() {
        synchronized (monitor) {
            return !activeEntries.isEmpty();
        }
    }

    boolean isDrainScheduled() {
        synchronized (monitor) {
            return drainScheduled;
        }
    }

    void beginSplit() {
        synchronized (monitor) {
            splitBlockCount++;
        }
    }

    void finishSplit() {
        synchronized (monitor) {
            if (splitBlockCount > 0) {
                splitBlockCount--;
            }
        }
    }

    DetachedOverlay<K, V> detachOverlaySnapshot() {
        synchronized (monitor) {
            final TreeMap<K, V> merged = new TreeMap<>(keyComparator);
            for (final PartitionImmutableRun<K, V> run : immutableRuns) {
                merged.putAll(run.getEntries());
            }
            merged.putAll(activeEntries);
            final int removedBufferedKeyCount = bufferedKeyCount;
            immutableRuns.clear();
            activeEntries = new TreeMap<>(keyComparator);
            bufferedKeyCount = 0;
            drainScheduled = false;
            splitBlockCount = 0;
            return new DetachedOverlay<>(merged, removedBufferedKeyCount);
        }
    }

    int restoreOverlaySnapshot(final NavigableMap<K, V> entries) {
        Vldtn.requireNonNull(entries, "entries");
        synchronized (monitor) {
            int restored = 0;
            for (final java.util.Map.Entry<K, V> entry : entries.entrySet()) {
                if (!activeEntries.containsKey(entry.getKey())) {
                    restored++;
                }
                activeEntries.put(entry.getKey(), entry.getValue());
            }
            bufferedKeyCount += restored;
            return restored;
        }
    }

    private boolean exceedsLocalCapacity(final PartitionRuntimeLimits limits) {
        if (bufferedKeyCount >= limits.getMaxNumberOfKeysInPartitionBuffer()) {
            return true;
        }
        return activeEntries
                .size() >= limits.getMaxNumberOfKeysInActivePartition()
                && immutableRuns.size() >= limits
                        .getMaxNumberOfImmutableRunsPerPartition();
    }

    private void rotateActiveUnsafe() {
        if (activeEntries.isEmpty()) {
            return;
        }
        immutableRuns.addLast(new PartitionImmutableRun<>(activeEntries));
        activeEntries = new TreeMap<>(keyComparator);
    }

    private static boolean reserveGlobal(
            final AtomicInteger totalBufferedKeyCount, final int maxGlobal) {
        while (true) {
            final int current = totalBufferedKeyCount.get();
            if (current >= maxGlobal) {
                return false;
            }
            if (totalBufferedKeyCount.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    static final class DetachedOverlay<K, V> {
        private final NavigableMap<K, V> entries;
        private final int removedBufferedKeyCount;

        private DetachedOverlay(final NavigableMap<K, V> entries,
                final int removedBufferedKeyCount) {
            this.entries = entries;
            this.removedBufferedKeyCount = removedBufferedKeyCount;
        }

        NavigableMap<K, V> getEntries() {
            return entries;
        }

        int getRemovedBufferedKeyCount() {
            return removedBufferedKeyCount;
        }
    }
}
