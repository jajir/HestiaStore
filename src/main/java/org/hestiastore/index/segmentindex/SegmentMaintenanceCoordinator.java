package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;

/**
 * Centralizes post-write maintenance decisions for a segment.
 */
final class SegmentMaintenanceCoordinator<K, V> {

    private final IndexConfiguration<K, V> conf;
    private final KeySegmentCache<K> keySegmentCache;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final SegmentAsyncSplitCoordinator<K, V> splitCoordinator;
    private final SegmentMaintenancePolicy<K, V> maintenancePolicy;

    SegmentMaintenanceCoordinator(final IndexConfiguration<K, V> conf,
            final KeySegmentCache<K> keySegmentCache,
            final SegmentRegistry<K, V> segmentRegistry) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.keySegmentCache = Vldtn.requireNonNull(keySegmentCache,
                "keySegmentCache");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.splitCoordinator = new SegmentAsyncSplitCoordinator<>(conf,
                keySegmentCache, segmentRegistry,
                segmentRegistry.getMaintenanceExecutor());
        this.maintenancePolicy = new SegmentMaintenancePolicyThreshold<>(
                conf.getMaxNumberOfKeysInSegmentWriteCache(),
                conf.getMaxNumberOfKeysInSegmentCache());
    }

    void handlePostWrite(final Segment<K, V> segment, final K key,
            final SegmentId segmentId, final long mappingVersion) {
        final Integer maxWriteCacheKeys = conf
                .getMaxNumberOfKeysInSegmentWriteCache();
        if (maxWriteCacheKeys == null || maxWriteCacheKeys < 1) {
            return;
        }
        if (segment.wasClosed()) {
            return;
        }
        if (!segmentRegistry.isSegmentInstance(segmentId, segment)) {
            return;
        }
        if (!keySegmentCache.isKeyMappedToSegment(key, segmentId)
                || !keySegmentCache.isMappingValid(key, segmentId,
                        mappingVersion)) {
            return;
        }
        scheduleMaintenanceIfNeeded(segment);

        final Integer maxSegmentCacheKeys = conf
                .getMaxNumberOfKeysInSegmentCache();
        if (maxSegmentCacheKeys != null && maxSegmentCacheKeys > 0) {
            final long totalKeys = segment.getNumberOfKeysInCache();
            if (totalKeys > maxSegmentCacheKeys.longValue()) {
                splitCoordinator.optionallySplitAsync(segment,
                        maxSegmentCacheKeys.longValue())
                        .toCompletableFuture()
                        .join();
            }
        }
    }

    private void scheduleMaintenanceIfNeeded(final Segment<K, V> segment) {
        final SegmentMaintenanceDecision decision = maintenancePolicy
                .evaluateAfterWrite(segment);
        if (decision.shouldFlush()) {
            segment.flush();
        }
        if (decision.shouldCompact()) {
            segment.compact();
        }
    }
}
