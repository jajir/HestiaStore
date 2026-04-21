package org.hestiastore.index.segmentindex.core.control;

import java.util.Map;

import org.hestiastore.index.control.model.RuntimeSettingKey;
import org.hestiastore.index.segment.SegmentRuntimeLimits;
import org.hestiastore.index.segmentregistry.SegmentHandle;
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

    public void apply(final Map<RuntimeSettingKey, Integer> effective) {
        final int maxSegmentsInCache = effective
                .get(RuntimeSettingKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE)
                .intValue();
        segmentRegistry.updateCacheLimit(maxSegmentsInCache);
        final int maxSegmentCache = effective
                .get(RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE)
                .intValue();
        final int maxSegmentWriteCache = effective.get(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION)
                .intValue();
        final int maxMaintenanceWriteCache = effective.get(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER)
                .intValue();
        final SegmentRuntimeLimits limits = new SegmentRuntimeLimits(
                maxSegmentCache, maxSegmentWriteCache,
                maxMaintenanceWriteCache);
        segmentRuntime.updateRuntimeLimits(limits);
        for (final SegmentHandle<K, V> segment : segmentRuntime
                .loadedSegmentsSnapshot()) {
            segment.getRuntime().updateRuntimeLimits(limits);
        }
    }
}
