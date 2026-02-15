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
    private final SegmentAsyncSplitCoordinator<K, V> splitCoordinator;

    SegmentMaintenanceCoordinator(final IndexConfiguration<K, V> conf,
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final SegmentAsyncSplitCoordinator<K, V> splitCoordinator) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.splitCoordinator = Vldtn.requireNonNull(splitCoordinator,
                "splitCoordinator");
    }

    void handlePostWrite(final Segment<K, V> segment, final K key,
            final SegmentId segmentId, final long mappingVersion) {
        if (Boolean.getBoolean("hestiastore.disableSplits")) {
            return;
        }
        final Integer maxWriteCacheKeys = conf
                .getMaxNumberOfKeysInSegmentWriteCache();
        if (maxWriteCacheKeys == null || maxWriteCacheKeys < 1) {
            return;
        }
        if (segment.getState() == SegmentState.CLOSED) {
            return;
        }
        if (!keyToSegmentMap.isKeyMappedToSegment(key, segmentId)
                || !keyToSegmentMap.isMappingValid(key, segmentId,
                        mappingVersion)) {
            return;
        }
        final Integer maxNumberOfKeysInSegment = conf
                .getMaxNumberOfKeysInSegment();
        if (maxNumberOfKeysInSegment != null
                && maxNumberOfKeysInSegment > 0) {
            final long totalKeys = segment.getNumberOfKeysInCache();
            if (totalKeys > maxNumberOfKeysInSegment.longValue()) {
                final SegmentAsyncSplitCoordinator.SplitHandle handle = splitCoordinator
                        .optionallySplitAsync(segment,
                                maxNumberOfKeysInSegment.longValue());
                handle.awaitCompletion(conf.getIndexBusyTimeoutMillis());
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
