package org.hestiastore.index.segmentindex.partition;

import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.split.SegmentSplitApplyPlan;
import org.hestiastore.index.segmentindex.split.SegmentSplitterResult;

/**
 * In-memory partitioned ingest overlay layered above stable segments.
 *
 * @param <K> key type
 * @param <V> value type
 * @author honza
 */
public final class PartitionRuntime<K, V> {

    private final Comparator<K> keyComparator;
    private final ConcurrentHashMap<SegmentId, RangePartition<K, V>> partitions = new ConcurrentHashMap<>();
    private final AtomicInteger totalBufferedKeyCount = new AtomicInteger();
    private final AtomicInteger localThrottleCount = new AtomicInteger();
    private final AtomicInteger globalThrottleCount = new AtomicInteger();
    private final AtomicInteger drainInFlightCount = new AtomicInteger();
    private final LongAdder drainScheduleCount = new LongAdder();

    public PartitionRuntime(final Comparator<K> keyComparator) {
        this.keyComparator = Vldtn.requireNonNull(keyComparator,
                "keyComparator");
    }

    public void ensurePartition(final SegmentId segmentId) {
        partition(segmentId);
    }

    public PartitionWriteResult write(final SegmentId segmentId, final K key,
            final V value, final PartitionRuntimeLimits limits) {
        Vldtn.requireNonNull(key, "key");
        Vldtn.requireNonNull(value, "value");
        return partition(segmentId).write(key, value, limits,
                totalBufferedKeyCount, localThrottleCount,
                globalThrottleCount);
    }

    public PartitionLookupResult<V> lookup(final SegmentId segmentId,
            final K key) {
        final RangePartition<K, V> partition = partitions.get(segmentId);
        if (partition == null) {
            return PartitionLookupResult.miss();
        }
        return partition.lookup(key);
    }

    public void sealAllActivePartitionsForDrain() {
        partitions.values().forEach(RangePartition::sealActiveForDrain);
    }

    public boolean markDrainScheduledIfNeeded(final SegmentId segmentId) {
        final RangePartition<K, V> partition = partitions.get(segmentId);
        if (partition == null || !partition.markDrainScheduledIfNeeded()) {
            return false;
        }
        drainScheduleCount.increment();
        drainInFlightCount.incrementAndGet();
        return true;
    }

    public PartitionImmutableRun<K, V> peekOldestImmutableRun(
            final SegmentId segmentId) {
        final RangePartition<K, V> partition = partitions.get(segmentId);
        if (partition == null) {
            return null;
        }
        return partition.peekOldestImmutableRun();
    }

    public void completeDrainedRun(final SegmentId segmentId,
            final PartitionImmutableRun<K, V> run) {
        final RangePartition<K, V> partition = partitions.get(segmentId);
        if (partition == null) {
            return;
        }
        partition.completeDrainedRun(run, totalBufferedKeyCount);
    }

    public void finishDrainScheduling(final SegmentId segmentId) {
        final RangePartition<K, V> partition = partitions.get(segmentId);
        if (partition == null) {
            return;
        }
        partition.finishDrainScheduling();
        final int updated = drainInFlightCount.decrementAndGet();
        if (updated < 0) {
            drainInFlightCount.compareAndSet(updated, 0);
        }
    }

    public void applyOverlaySnapshot(final List<SegmentId> segmentIds,
            final NavigableMap<K, V> target) {
        Vldtn.requireNonNull(segmentIds, "segmentIds");
        Vldtn.requireNonNull(target, "target");
        for (final SegmentId segmentId : segmentIds) {
            final RangePartition<K, V> partition = partitions.get(segmentId);
            if (partition != null) {
                partition.applyOverlaySnapshot(target);
            }
        }
    }

    public PartitionRuntimeSnapshot snapshot() {
        int activePartitionCount = 0;
        int drainingPartitionCount = 0;
        int immutableRunCount = 0;
        for (final RangePartition<K, V> partition : partitions.values()) {
            if (partition.hasActiveEntries()) {
                activePartitionCount++;
            }
            if (partition.isDrainScheduled()) {
                drainingPartitionCount++;
            }
            immutableRunCount += partition.getImmutableRunCount();
        }
        return new PartitionRuntimeSnapshot(partitions.size(),
                activePartitionCount, drainingPartitionCount, immutableRunCount,
                totalBufferedKeyCount.get(), localThrottleCount.get(),
                globalThrottleCount.get(), drainScheduleCount.sum(),
                Math.max(0, drainInFlightCount.get()));
    }

    public boolean hasBufferedData() {
        return totalBufferedKeyCount.get() > 0;
    }

    public void beginSplit(final SegmentId segmentId) {
        partition(segmentId).beginSplit();
    }

    public void finishSplit(final SegmentId segmentId) {
        final RangePartition<K, V> partition = partitions.get(segmentId);
        if (partition == null) {
            return;
        }
        partition.finishSplit();
    }

    public void reassignOverlayAfterSplit(final SegmentSplitApplyPlan<K> plan) {
        Vldtn.requireNonNull(plan, "plan");
        final RangePartition<K, V> oldPartition = partitions
                .remove(plan.getOldSegmentId());
        if (oldPartition == null) {
            return;
        }
        final RangePartition.DetachedOverlay<K, V> detached = oldPartition
                .detachOverlaySnapshot();
        totalBufferedKeyCount
                .addAndGet(-detached.getRemovedBufferedKeyCount());
        final NavigableMap<K, V> detachedEntries = detached.getEntries();
        if (detachedEntries.isEmpty()) {
            return;
        }
        if (plan.getStatus() == SegmentSplitterResult.SegmentSplittingStatus.SPLIT) {
            final NavigableMap<K, V> lowerEntries = new TreeMap<>(
                    keyComparator);
            final NavigableMap<K, V> upperEntries = new TreeMap<>(
                    keyComparator);
            detachedEntries.forEach((key, value) -> {
                if (keyComparator.compare(key, plan.getMaxKey()) <= 0) {
                    lowerEntries.put(key, value);
                } else {
                    upperEntries.put(key, value);
                }
            });
            restoreOverlay(plan.getLowerSegmentId(), lowerEntries);
            plan.getUpperSegmentId()
                    .ifPresent(upperSegmentId -> restoreOverlay(upperSegmentId,
                            upperEntries));
            return;
        }
        restoreOverlay(plan.getLowerSegmentId(), detachedEntries);
    }

    private RangePartition<K, V> partition(final SegmentId segmentId) {
        return partitions.computeIfAbsent(
                Vldtn.requireNonNull(segmentId, "segmentId"),
                ignored -> new RangePartition<>(keyComparator));
    }

    private void restoreOverlay(final SegmentId segmentId,
            final NavigableMap<K, V> entries) {
        if (entries.isEmpty()) {
            return;
        }
        final int restored = partition(segmentId).restoreOverlaySnapshot(
                entries);
        totalBufferedKeyCount.addAndGet(restored);
    }
}
