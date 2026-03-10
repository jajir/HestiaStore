package org.hestiastore.index.segmentindex.core;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.split.SegmentAsyncSplitCoordinator;

/**
 * Coordinates explicit split scheduling after partition drain or maintenance
 * boundaries.
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

    void handlePostDrain(final Segment<K, V> segment,
            final long maxNumberOfKeysInPartitionBeforeSplit) {
        if (segment == null || segment.getState() == SegmentState.CLOSED) {
            return;
        }
        if (!isSplitSchedulingEnabled(maxNumberOfKeysInPartitionBeforeSplit)) {
            return;
        }
        if (!keyToSegmentMap.getSegmentIds().contains(segment.getId())) {
            return;
        }
        optionallyScheduleSplit(segment, maxNumberOfKeysInPartitionBeforeSplit);
    }

    void awaitSplitsIdle(final long timeoutMillis) {
        splitCoordinator.awaitAllCompletions(timeoutMillis);
    }

    int splitInFlightCount() {
        return splitCoordinator.inFlightCount();
    }

    private void optionallyScheduleSplit(final Segment<K, V> segment,
            final long splitThreshold) {
        if (!isSplitSchedulingEnabled(splitThreshold)) {
            return;
        }
        final long totalKeys = segment.getNumberOfKeysInCache();
        if (totalKeys > splitThreshold) {
            splitCoordinator.optionallySplitAsync(segment, splitThreshold);
        }
    }

    private boolean isSplitSchedulingEnabled(final long splitThreshold) {
        if (Boolean.getBoolean("hestiastore.disableSplits")) {
            return false;
        }
        final Integer maxActivePartitionKeys = conf
                .getMaxNumberOfKeysInActivePartition();
        return maxActivePartitionKeys != null && maxActivePartitionKeys >= 1
                && splitThreshold >= 1L;
    }
}
