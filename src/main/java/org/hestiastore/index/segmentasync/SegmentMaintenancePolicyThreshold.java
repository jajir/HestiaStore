package org.hestiastore.index.segmentasync;

import org.hestiastore.index.segment.Segment;

/**
 * Simple maintenance policy based on write cache size.
 */
public final class SegmentMaintenancePolicyThreshold<K, V>
        implements SegmentMaintenancePolicy<K, V> {

    private final Integer maxWriteCacheKeys;

    public SegmentMaintenancePolicyThreshold(final Integer maxWriteCacheKeys) {
        this.maxWriteCacheKeys = maxWriteCacheKeys;
    }

    @Override
    public SegmentMaintenanceDecision evaluateAfterWrite(
            final Segment<K, V> segment) {
        if (maxWriteCacheKeys == null || maxWriteCacheKeys.intValue() < 1) {
            return SegmentMaintenanceDecision.none();
        }
        if (segment.getNumberOfKeysInWriteCache() >= maxWriteCacheKeys
                .intValue()) {
            return SegmentMaintenanceDecision.flushOnly();
        }
        return SegmentMaintenanceDecision.none();
    }
}
