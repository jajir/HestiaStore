package org.hestiastore.index.segmentindex.core.split;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentregistry.BlockingSegment;

/**
 * Threshold-based split policy that uses the in-cache key count.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexSplitPolicyThreshold<K, V>
        implements SegmentIndexSplitPolicy<K, V> {

    /**
     * Returns true when the in-memory key count reaches the configured
     * threshold.
     *
     * @param segmentHandle evaluated segment
     * @param maxNumberOfKeysInSegment split threshold
     * @return true when split should be triggered
     */
    @Override
    public boolean shouldSplit(final BlockingSegment<K, V> segmentHandle,
            final long maxNumberOfKeysInSegment) {
        Vldtn.requireNonNull(segmentHandle, "segmentHandle");
        if (maxNumberOfKeysInSegment < 1) {
            return false;
        }
        return segmentHandle.getRuntime()
                .getNumberOfKeysInCache() >= maxNumberOfKeysInSegment;
    }
}
