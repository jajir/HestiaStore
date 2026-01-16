package org.hestiastore.index.segment;

import org.hestiastore.index.Vldtn;

/**
 * Simple maintenance policy based on segment cache and write cache sizes.
 */
public final class SegmentMaintenancePolicyThreshold<K, V>
        implements SegmentMaintenancePolicy<K, V> {

    private final int maxSegmentCacheKeys;
    private final int maxWriteCacheKeys;

    public SegmentMaintenancePolicyThreshold(final int maxSegmentCacheKeys,
            final int maxWriteCacheKeys) {
        this.maxSegmentCacheKeys = Vldtn.requireBetween(maxSegmentCacheKeys, 0,
                Integer.MAX_VALUE, "maxSegmentCacheKeys");
        this.maxWriteCacheKeys = Vldtn.requireBetween(maxWriteCacheKeys, 0,
                Integer.MAX_VALUE, "maxWriteCacheKeys");
    }

    @Override
    public SegmentMaintenanceDecision evaluateAfterWrite(
            final Segment<K, V> segment) {
        Vldtn.requireNonNull(segment, "segment");
        if (maxSegmentCacheKeys > 0 && segment
                .getNumberOfKeysInSegmentCache() >= maxSegmentCacheKeys) {
            return SegmentMaintenanceDecision.compactOnly();
        }
        if (maxWriteCacheKeys > 0
                && segment.getNumberOfKeysInWriteCache() >= maxWriteCacheKeys) {
            return SegmentMaintenanceDecision.flushOnly();
        }
        return SegmentMaintenanceDecision.none();
    }
}
