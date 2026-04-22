package org.hestiastore.index.segmentindex.core.routing;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RouteSplitPublishCoordinatorTest {

    private static final SegmentId PARENT_SEGMENT_ID = SegmentId.of(1);
    private static final SegmentId LOWER_SEGMENT_ID = SegmentId.of(2);
    private static final SegmentId UPPER_SEGMENT_ID = SegmentId.of(3);

    @Mock
    private KeyToSegmentMap<Integer> keyToSegmentMap;

    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;

    @Mock
    private DefaultSegmentMaterializationService<Integer, String> materializationService;

    private RouteSplitPublishCoordinator<Integer, String> coordinator;
    private RouteSplitPlan<Integer> splitPlan;

    @BeforeEach
    void setUp() {
        coordinator = new RouteSplitPublishCoordinator<>(keyToSegmentMap,
                segmentRegistry, materializationService);
        splitPlan = new RouteSplitPlan<>(PARENT_SEGMENT_ID, LOWER_SEGMENT_ID,
                UPPER_SEGMENT_ID, 1, 2, RouteSplitPlan.SplitMode.SPLIT);
    }

    @Test
    void applyPreparedSplitReturnsMapResult() {
        when(keyToSegmentMap.tryApplySplitPlan(splitPlan)).thenReturn(true);

        final boolean published = coordinator.applyPreparedSplit(splitPlan);

        assertTrue(published);
    }

    @Test
    void applyPreparedSplitReturnsFalseOnMapFailure() {
        when(keyToSegmentMap.tryApplySplitPlan(splitPlan))
                .thenThrow(new IllegalStateException("boom"));

        final boolean published = coordinator.applyPreparedSplit(splitPlan);

        assertFalse(published);
    }

    @Test
    void applyPreparedSplitFlushesMapBeforeDeletingParent() {
        when(keyToSegmentMap.tryApplySplitPlan(splitPlan)).thenReturn(true);
        when(segmentRegistry.deleteSegmentIfAvailable(PARENT_SEGMENT_ID))
                .thenReturn(true);

        coordinator.applyPreparedSplit(splitPlan);

        final InOrder inOrder = inOrder(keyToSegmentMap, segmentRegistry);
        inOrder.verify(keyToSegmentMap).tryApplySplitPlan(splitPlan);
        inOrder.verify(keyToSegmentMap).flushIfDirty();
        inOrder.verify(segmentRegistry)
                .deleteSegmentIfAvailable(PARENT_SEGMENT_ID);
    }

    @Test
    void applyPreparedSplitDeletesMaterializedChildrenOnMapFailure() {
        when(keyToSegmentMap.tryApplySplitPlan(splitPlan)).thenReturn(false);

        coordinator.applyPreparedSplit(splitPlan);

        verify(materializationService).deletePreparedSegment(LOWER_SEGMENT_ID);
        verify(materializationService).deletePreparedSegment(UPPER_SEGMENT_ID);
    }
}
