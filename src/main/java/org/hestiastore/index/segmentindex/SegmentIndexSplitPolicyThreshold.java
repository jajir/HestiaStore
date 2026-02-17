package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;

/**
 * Threshold-based split policy that uses the in-cache key count.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentIndexSplitPolicyThreshold<K, V>
        implements SegmentIndexSplitPolicy<K, V> {

    /**
     * Returns true when the in-memory key count reaches the configured
     * threshold.
     *
     * @param segment evaluated segment
     * @param maxNumberOfKeysInSegment split threshold
     * @return true when split should be triggered
     */
    @Override
    public boolean shouldSplit(final Segment<K, V> segment,
            final long maxNumberOfKeysInSegment) {
        Vldtn.requireNonNull(segment, "segment");
        if (maxNumberOfKeysInSegment < 1) {
            return false;
        }
        return segment.getNumberOfKeysInCache() >= maxNumberOfKeysInSegment;
    }
}
