package org.hestiastore.index.segmentasync;

import org.hestiastore.index.segment.Segment;

/**
 * Simple maintenance policy based on write and delta cache sizes.
 */
public final class SegmentMaintenancePolicyThreshold<K, V>
        implements SegmentMaintenancePolicy<K, V> {

    private final Integer maxWriteCacheKeys;
    private final Integer maxDeltaCacheKeys;

    public SegmentMaintenancePolicyThreshold(final Integer maxWriteCacheKeys) {
        this(maxWriteCacheKeys, null);
    }

    public SegmentMaintenancePolicyThreshold(final Integer maxWriteCacheKeys,
            final Integer maxDeltaCacheKeys) {
        this.maxWriteCacheKeys = maxWriteCacheKeys;
        this.maxDeltaCacheKeys = maxDeltaCacheKeys;
    }

    @Override
    public SegmentMaintenanceDecision evaluateAfterWrite(
            final Segment<K, V> segment) {
        final boolean flush = maxWriteCacheKeys != null
                && maxWriteCacheKeys.intValue() > 0
                && segment.getNumberOfKeysInWriteCache() >= maxWriteCacheKeys
                        .intValue();
        final boolean compact = maxDeltaCacheKeys != null
                && maxDeltaCacheKeys.intValue() > 0
                && segment.getStats().getNumberOfKeysInDeltaCache()
                        >= maxDeltaCacheKeys.intValue();
        if (flush && compact) {
            return SegmentMaintenanceDecision.flushAndCompact();
        }
        if (flush) {
            return SegmentMaintenanceDecision.flushOnly();
        }
        if (compact) {
            return SegmentMaintenanceDecision.compactOnly();
        }
        return SegmentMaintenanceDecision.none();
    }
}
