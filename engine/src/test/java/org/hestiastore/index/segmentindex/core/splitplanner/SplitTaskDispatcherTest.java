package org.hestiastore.index.segmentindex.core.splitplanner;

import org.hestiastore.index.OperationResult;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;

import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.core.metrics.Stats;
import org.hestiastore.index.segmentindex.core.routing.BackgroundSplitCoordinator;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentHandle;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SplitTaskDispatcherTest {

    @Mock
    private KeyToSegmentMap<Integer> keyToSegmentMap;

    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;

    @Mock
    private BackgroundSplitCoordinator<Integer, String> backgroundSplitCoordinator;

    @Mock
    private SegmentHandle<Integer, String> segmentHandle;

    private Stats stats;
    private SplitTaskDispatcher<Integer, String> dispatcher;

    @BeforeEach
    void setUp() {
        stats = new Stats();
        dispatcher = new SplitTaskDispatcher<>(keyToSegmentMap, segmentRegistry,
                backgroundSplitCoordinator, stats);
    }

    @Test
    void dispatchCandidatesSchedulesEligibleMappedSegments() {
        final SegmentId segmentId = SegmentId.of(7);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.of(segmentHandle));
        when(backgroundSplitCoordinator.handleSplitCandidate(segmentHandle, 10,
                false)).thenReturn(true);

        dispatcher.dispatchCandidates(List.of(segmentId), 10, false);

        verify(backgroundSplitCoordinator).handleSplitCandidate(segmentHandle,
                10, false);
        assertEquals(1L, stats.getSplitScheduleCount());
    }

    @Test
    void dispatchCandidatesSkipsUnloadedSegments() {
        final SegmentId segmentId = SegmentId.of(8);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.empty());

        dispatcher.dispatchCandidates(List.of(segmentId), 10, false);

        verify(segmentRegistry).tryGetSegment(segmentId);
        verify(backgroundSplitCoordinator, never()).handleSplitCandidate(
                segmentHandle, 10, false);
        assertEquals(0L, stats.getSplitScheduleCount());
    }
}
