package org.hestiastore.index.segmentindex.core;

import java.util.Map;

import org.hestiastore.index.control.model.RuntimeSettingKey;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentRuntimeLimits;
import org.hestiastore.index.segmentregistry.SegmentFactory;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Applies runtime tuning changes to segment registry cache limits and all
 * loaded stable segments.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentRuntimeLimitApplier<K, V> {

    private final SegmentRegistry<K, V> segmentRegistry;
    private final SegmentFactory<K, V> segmentFactory;

    SegmentRuntimeLimitApplier(final SegmentRegistry<K, V> segmentRegistry,
            final SegmentFactory<K, V> segmentFactory) {
        this.segmentRegistry = segmentRegistry;
        this.segmentFactory = segmentFactory;
    }

    void apply(final Map<RuntimeSettingKey, Integer> effective) {
        final int maxSegmentsInCache = effective
                .get(RuntimeSettingKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE)
                .intValue();
        segmentRegistry.updateCacheLimit(maxSegmentsInCache);
        final int maxSegmentCache = effective
                .get(RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE)
                .intValue();
        final int maxActivePartition = effective.get(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION)
                .intValue();
        final int maxPartitionBuffer = effective.get(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER)
                .intValue();
        segmentFactory.updateRuntimeLimits(maxSegmentCache, maxActivePartition,
                maxPartitionBuffer);
        final SegmentRuntimeLimits limits = new SegmentRuntimeLimits(
                maxSegmentCache, maxActivePartition, maxPartitionBuffer);
        for (final Segment<K, V> segment : segmentRegistry
                .loadedSegmentsSnapshot()) {
            segment.applyRuntimeLimits(limits);
        }
    }
}
