package org.hestiastore.index.segmentregistry;

import java.util.concurrent.ExecutorService;
import java.util.function.BooleanSupplier;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentBuilder;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.SegmentSplitApplyPlan;

/**
 * Internal contract for registry operations needed by split/maintenance flows.
 * This interface does not extend {@link SegmentRegistry} to keep the public API minimal.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SegmentRegistryMaintenance<K, V> {

    ExecutorService getSplitExecutor();

    void markSplitInFlight(SegmentId segmentId);

    void clearSplitInFlight(SegmentId segmentId);

    boolean isSegmentInstance(SegmentId segmentId, Segment<K, V> expected);

    SegmentHandlerLockStatus lockSegmentHandler(SegmentId segmentId,
            Segment<K, V> expected);

    void unlockSegmentHandler(SegmentId segmentId, Segment<K, V> expected);

    SegmentBuilder<K, V> newSegmentBuilder(SegmentId segmentId);

    SegmentRegistryResult<Segment<K, V>> applySplitPlan(
            SegmentSplitApplyPlan<K, V> plan, Segment<K, V> lowerSegment,
            Segment<K, V> upperSegment, BooleanSupplier onApplied);

    void closeSegmentInstance(Segment<K, V> segment);

    void deleteSegmentFiles(SegmentId segmentId);
}
