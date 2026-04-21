package org.hestiastore.index.segmentindex.core.observability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentRuntimeSnapshot;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentHandle;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StableSegmentRuntimeCollectorTest {

    @Mock
    private KeyToSegmentMap<Integer> keyToSegmentMap;

    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;

    @Mock
    private SegmentRegistry.Runtime<Integer, String> segmentRegistryRuntime;

    @Mock
    private SegmentHandle<Integer, String> readySegmentHandle;

    @Mock
    private SegmentHandle.Runtime readySegmentRuntime;

    @Mock
    private SegmentHandle<Integer, String> maintenanceSegmentHandle;

    @Mock
    private SegmentHandle.Runtime maintenanceSegmentRuntime;

    @Mock
    private SegmentHandle<Integer, String> unmappedSegmentHandle;

    @Mock
    private SegmentHandle.Runtime unmappedSegmentRuntime;

    private StableSegmentRuntimeCollector<Integer, String> collector;

    @BeforeEach
    void setUp() {
        collector = new StableSegmentRuntimeCollector<>(keyToSegmentMap,
                segmentRegistry);
    }

    @Test
    void collectAggregatesOnlyMappedLoadedSegments() {
        when(segmentRegistry.runtime()).thenReturn(segmentRegistryRuntime);
        when(keyToSegmentMap.getSegmentIds())
                .thenReturn(List.of(SegmentId.of(1), SegmentId.of(2),
                        SegmentId.of(3)));
        when(segmentRegistryRuntime.loadedSegmentsSnapshot()).thenReturn(
                Arrays.asList(readySegmentHandle, maintenanceSegmentHandle,
                        unmappedSegmentHandle, null));

        when(readySegmentHandle.getRuntime()).thenReturn(readySegmentRuntime);
        when(readySegmentRuntime.getRuntimeSnapshot()).thenReturn(
                segmentRuntimeSnapshot(1, SegmentState.READY, 10L, 3L, 2, 1, 4L,
                        6L, 7L, 1L));

        when(maintenanceSegmentHandle.getRuntime())
                .thenReturn(maintenanceSegmentRuntime);
        when(maintenanceSegmentRuntime.getRuntimeSnapshot()).thenReturn(
                segmentRuntimeSnapshot(2, SegmentState.FREEZE, 5L, 1L, 1, 2, 2L,
                        3L, 4L, 0L));

        when(unmappedSegmentHandle.getRuntime()).thenReturn(unmappedSegmentRuntime);
        when(unmappedSegmentRuntime.getRuntimeSnapshot()).thenReturn(
                segmentRuntimeSnapshot(99, SegmentState.CLOSED, 100L, 50L, 10,
                        10, 10L, 10L, 10L, 10L));

        final StableSegmentRuntimeMetrics metrics = collector.collect();

        assertEquals(3, metrics.getTotalMappedStableSegmentCount());
        assertEquals(1, metrics.getReadyStableSegmentCount());
        assertEquals(1, metrics.getStableSegmentsInMaintenanceStateCount());
        assertEquals(0, metrics.getErrorStableSegmentCount());
        assertEquals(0, metrics.getClosedStableSegmentCount());
        assertEquals(1, metrics.getUnloadedMappedStableSegmentCount());
        assertEquals(15L, metrics.getTotalStableSegmentKeyCount());
        assertEquals(4L, metrics.getTotalStableSegmentCacheKeyCount());
        assertEquals(3L, metrics.getTotalStableSegmentWriteBufferKeyCount());
        assertEquals(3L, metrics.getTotalStableSegmentDeltaCacheFileCount());
        assertEquals(6L, metrics.getTotalCompactRequestCount());
        assertEquals(9L, metrics.getTotalFlushRequestCount());
        assertEquals(11L, metrics.getTotalBloomFilterRequestCount());
        assertEquals(0L, metrics.getTotalBloomFilterRefusedCount());
        assertEquals(10L, metrics.getTotalBloomFilterPositiveCount());
        assertEquals(1L, metrics.getTotalBloomFilterFalsePositiveCount());
        assertEquals(2, metrics.getStableSegmentMetricsSnapshots().size());
    }

    @Test
    void collectReturnsEmptyAggregateWhenNoSegmentsAreMapped() {
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of());

        final StableSegmentRuntimeMetrics metrics = collector.collect();

        assertEquals(0, metrics.getTotalMappedStableSegmentCount());
        assertEquals(0, metrics.getUnloadedMappedStableSegmentCount());
        assertEquals(0, metrics.getStableSegmentMetricsSnapshots().size());
    }

    private static SegmentRuntimeSnapshot segmentRuntimeSnapshot(
            final int segmentId, final SegmentState state, final long keyCount,
            final long cacheKeyCount, final int writeCacheKeyCount,
            final int deltaCacheFileCount, final long compactCount,
            final long flushCount, final long bloomFilterRequestCount,
            final long bloomFilterFalsePositiveCount) {
        return new SegmentRuntimeSnapshot(SegmentId.of(segmentId), state, 0L,
                keyCount, 0L, cacheKeyCount, writeCacheKeyCount,
                deltaCacheFileCount, compactCount, flushCount,
                bloomFilterRequestCount, 0L, bloomFilterRequestCount
                        - bloomFilterFalsePositiveCount,
                bloomFilterFalsePositiveCount);
    }
}
