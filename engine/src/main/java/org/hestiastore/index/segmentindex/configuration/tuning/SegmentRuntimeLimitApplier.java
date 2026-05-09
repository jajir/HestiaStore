package org.hestiastore.index.segmentindex.configuration.tuning;

import org.hestiastore.index.segment.SegmentRuntimeLimits;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Applies runtime tuning changes to segment registry cache limits and all
 * loaded stable segments.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentRuntimeLimitApplier<K, V> {

    private final SegmentRegistry<K, V> segmentRegistry;
    private final SegmentRegistry.Runtime<K, V> segmentRuntime;

    public SegmentRuntimeLimitApplier(
            final SegmentRegistry<K, V> segmentRegistry,
            final SegmentRegistry.Runtime<K, V> segmentRuntime) {
        this.segmentRegistry = segmentRegistry;
        this.segmentRuntime = segmentRuntime;
    }

    public void apply(final RuntimeTuningSnapshot effective) {
        final int maxSegmentsInCache = effective.segment().cachedSegmentLimit();
        segmentRegistry.updateCacheLimit(maxSegmentsInCache);
        final int maxSegmentCache = effective.segment().cacheKeyLimit();
        final int maxSegmentWriteCache = effective.writePath()
                .segmentWriteCacheKeyLimit();
        final int maxMaintenanceWriteCache = effective.writePath()
                .segmentWriteCacheKeyLimitDuringMaintenance();
        final SegmentRuntimeLimits limits = new SegmentRuntimeLimits(
                maxSegmentCache, maxSegmentWriteCache,
                maxMaintenanceWriteCache);
        segmentRuntime.updateRuntimeLimits(limits);
        for (final BlockingSegment<K, V> segment : segmentRuntime
                .loadedSegmentsSnapshot()) {
            segment.getRuntime().updateRuntimeLimits(limits);
        }
    }
}
