package org.hestiastore.index.segment;

import org.hestiastore.index.Vldtn;

/**
 * Simple maintenance policy based on segment cache, write cache, and delta
 * cache file thresholds.
 */
public final class SegmentMaintenancePolicyThreshold<K, V>
        implements SegmentMaintenancePolicy<K, V> {

    private final int maxSegmentCacheKeys;
    private final int maxWriteCacheKeys;
    private final int maxDeltaCacheFiles;

    /**
     * Creates a threshold-based maintenance policy.
     *
     * @param maxSegmentCacheKeys max cached keys before compaction is requested
     * @param maxWriteCacheKeys   max write-cache keys before flush is requested
     * @param maxDeltaCacheFiles  max delta cache files before compaction is
     *                            requested
     */
    public SegmentMaintenancePolicyThreshold(final int maxSegmentCacheKeys,
            final int maxWriteCacheKeys, final int maxDeltaCacheFiles) {
        this.maxSegmentCacheKeys = Vldtn.requireBetween(maxSegmentCacheKeys, 0,
                Integer.MAX_VALUE, "maxSegmentCacheKeys");
        this.maxWriteCacheKeys = Vldtn.requireBetween(maxWriteCacheKeys, 0,
                Integer.MAX_VALUE, "maxWriteCacheKeys");
        this.maxDeltaCacheFiles = Vldtn.requireBetween(maxDeltaCacheFiles, 0,
                Integer.MAX_VALUE, "maxDeltaCacheFiles");
    }

    /**
     * Evaluates the segment cache sizes and delta cache file count and returns
     * a maintenance decision.
     *
     * @param segment segment to evaluate
     * @return decision based on configured thresholds
     */
    @Override
    public SegmentMaintenanceDecision evaluateAfterWrite(
            final Segment<K, V> segment) {
        Vldtn.requireNonNull(segment, "segment");
        if (segment.getNumberOfKeysInSegmentCache() >= maxSegmentCacheKeys) {
            return SegmentMaintenanceDecision.compactOnly();
        }
        if (segment.getNumberOfDeltaCacheFiles() >= maxDeltaCacheFiles) {
            return SegmentMaintenanceDecision.compactOnly();
        }
        if (segment.getNumberOfKeysInWriteCache() >= maxWriteCacheKeys) {
            return SegmentMaintenanceDecision.flushOnly();
        }
        return SegmentMaintenanceDecision.none();
    }
}
