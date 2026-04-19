package org.hestiastore.index.segmentindex.split;

import org.hestiastore.index.segmentregistry.SegmentHandle;

/**
 * Decides when a segment should be split by the segment-index layer.
 *
 * @param <K> key type
 * @param <V> value type
 */
interface SegmentIndexSplitPolicy<K, V> {

    /**
     * Returns whether the segment should be split for the given threshold.
     *
     * @param segmentHandle segment to evaluate
     * @param maxNumberOfKeysInSegment split threshold
     * @return true when a split should be scheduled
     */
    boolean shouldSplit(SegmentHandle<K, V> segmentHandle,
            long maxNumberOfKeysInSegment);

    /**
     * Returns a policy that never schedules splits.
     *
     * @param <K> key type
     * @param <V> value type
     * @return policy that always returns false
     */
    static <K, V> SegmentIndexSplitPolicy<K, V> none() {
        return (segment, maxNumberOfKeysInSegment) -> false;
    }
}
