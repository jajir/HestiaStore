package org.hestiastore.index.segmentindex.core;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.partition.PartitionRuntime;
import org.hestiastore.index.segmentindex.split.PartitionStableSplitCoordinator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentMaintenanceCoordinatorTest {

    @Mock
    private KeyToSegmentMap<String> keyToSegmentMap;

    private KeyToSegmentMapSynchronizedAdapter<String> synchronizedKeyToSegmentMap;

    @Mock
    private Segment<String, String> segment;

    private PartitionRuntime<String, String> partitionRuntime;

    @Mock
    private PartitionStableSplitCoordinator<String, String> splitCoordinator;

    @BeforeEach
    void setUp() {
        synchronizedKeyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                keyToSegmentMap);
        partitionRuntime = new PartitionRuntime<>(String::compareTo);
    }

    @Test
    void returnsEarlyWhenSegmentIsClosed() {
        when(segment.getState()).thenReturn(SegmentState.CLOSED);

        final SegmentMaintenanceCoordinator<String, String> coordinator = new SegmentMaintenanceCoordinator<>(
                synchronizedKeyToSegmentMap, partitionRuntime,
                splitCoordinator);

        coordinator.handlePostDrain(segment, 100L);

        verifyNoInteractions(keyToSegmentMap, splitCoordinator);
        verify(segment, never()).getNumberOfKeysInCache();
    }

    @Test
    void usesSegmentSizeThreshold_notSegmentCacheThreshold() {
        final SegmentId segmentId = SegmentId.of(1);
        when(segment.getId()).thenReturn(segmentId);
        when(segment.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        when(segment.getNumberOfKeysInCache()).thenReturn(50L);

        final SegmentMaintenanceCoordinator<String, String> coordinator = new SegmentMaintenanceCoordinator<>(
                synchronizedKeyToSegmentMap, partitionRuntime,
                splitCoordinator);

        coordinator.handlePostDrain(segment, 1000L);

        verify(segment).getNumberOfKeysInCache();
        verifyNoInteractions(splitCoordinator);
    }

    @Test
    void schedulesSplitAfterDrainForStillMappedSegment() {
        final SegmentId segmentId = SegmentId.of(1);
        when(segment.getId()).thenReturn(segmentId);
        when(segment.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        when(segment.getNumberOfKeysInCache()).thenReturn(101L);

        final SegmentMaintenanceCoordinator<String, String> coordinator = new SegmentMaintenanceCoordinator<>(
                synchronizedKeyToSegmentMap, partitionRuntime,
                splitCoordinator);

        coordinator.handlePostDrain(segment, 100L);

        verify(splitCoordinator).optionallySplit(eq(segment), eq(100L), any());
    }

}
