package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;

/**
 * Coordinates post-write maintenance triggers and split decisions.
 */
final class SegmentMaintenanceCoordinator<K, V> {

    private final IndexConfiguration<K, V> conf;
    private final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap;
    private final SegmentRegistryImpl<K, V> segmentRegistry;
    private final SegmentAsyncSplitCoordinator<K, V> splitCoordinator;

    SegmentMaintenanceCoordinator(final IndexConfiguration<K, V> conf,
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final SegmentRegistryImpl<K, V> segmentRegistry) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.splitCoordinator = new SegmentAsyncSplitCoordinator<>(conf,
                keyToSegmentMap, segmentRegistry,
                segmentRegistry.getSplitExecutor());
    }

    void handlePostWrite(final Segment<K, V> segment, final K key,
            final SegmentId segmentId, final long mappingVersion) {
        final Integer maxWriteCacheKeys = conf
                .getMaxNumberOfKeysInSegmentWriteCache();
        if (maxWriteCacheKeys == null || maxWriteCacheKeys < 1) {
            return;
        }
        if (segment.getState() == SegmentState.CLOSED) {
            return;
        }
        if (!segmentRegistry.isSegmentInstance(segmentId, segment)) {
            return;
        }
        if (!keyToSegmentMap.isKeyMappedToSegment(key, segmentId)
                || !keyToSegmentMap.isMappingValid(key, segmentId,
                        mappingVersion)) {
            return;
        }
        final Integer maxSegmentCacheKeys = conf
                .getMaxNumberOfKeysInSegmentCache();
        if (maxSegmentCacheKeys != null && maxSegmentCacheKeys > 0) {
            final long totalKeys = segment.getNumberOfKeysInCache();
            if (totalKeys > maxSegmentCacheKeys.longValue()) {
                final SegmentAsyncSplitCoordinator.SplitHandle handle = splitCoordinator
                        .optionallySplitAsync(segment,
                                maxSegmentCacheKeys.longValue());
                handle.awaitStarted(conf.getIndexBusyTimeoutMillis());
            }
        }
    }

    void awaitSplitCompletionIfInFlight(final SegmentId segmentId,
            final long timeoutMillis) {
        splitCoordinator.awaitCompletionIfInFlight(segmentId, timeoutMillis);
    }

    void awaitSplitsIdle(final long timeoutMillis) {
        splitCoordinator.awaitAllCompletions(timeoutMillis);
    }
}
