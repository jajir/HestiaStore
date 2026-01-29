package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentregistry.SegmentRegistryImpl;
import org.hestiastore.index.segmentregistry.SegmentRegistryResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentSplitCoordinatorApplyPlanOrderTest {

    @Mock
    private KeyToSegmentMapSynchronizedAdapter<Integer> keyToSegmentMap;

    @Mock
    private SegmentRegistryImpl<Integer, String> segmentRegistry;

    @Mock
    private Segment<Integer, String> removedSegment;

    private SegmentSplitCoordinator<Integer, String> coordinator;

    @BeforeEach
    void setUp() {
        coordinator = new SegmentSplitCoordinator<>(
                IndexConfiguration.<Integer, String>builder().build(),
                keyToSegmentMap, segmentRegistry);
    }

    @AfterEach
    void tearDown() {
        coordinator = null;
    }

    @Test
    void applySplitPlan_persists_map_after_registry_apply() {
        final SegmentSplitApplyPlan<Integer, String> plan = new SegmentSplitApplyPlan<>(
                SegmentId.of(1), SegmentId.of(2), SegmentId.of(3), 1, 10,
                SegmentSplitterResult.SegmentSplittingStatus.SPLIT);
        when(keyToSegmentMap.applySplitPlan(plan)).thenReturn(true);
        when(segmentRegistry.applySplitPlan(eq(plan), isNull(), isNull(),
                any())).thenAnswer(invocation -> {
                    final java.util.function.BooleanSupplier callback = invocation
                            .getArgument(3);
                    if (callback != null) {
                        callback.getAsBoolean();
                    }
                    return SegmentRegistryResult.ok(removedSegment);
                });

        assertTrue(coordinator.applySplitPlan(plan).isOk());

        final InOrder inOrder = inOrder(segmentRegistry, keyToSegmentMap);
        inOrder.verify(segmentRegistry).applySplitPlan(eq(plan), isNull(),
                isNull(), any());
        inOrder.verify(keyToSegmentMap).applySplitPlan(plan);
        inOrder.verify(keyToSegmentMap).optionalyFlush();
    }

    @Test
    void applySplitPlan_skips_map_update_when_registry_busy() {
        final SegmentSplitApplyPlan<Integer, String> plan = new SegmentSplitApplyPlan<>(
                SegmentId.of(1), SegmentId.of(2), SegmentId.of(3), 1, 10,
                SegmentSplitterResult.SegmentSplittingStatus.SPLIT);
        when(segmentRegistry.applySplitPlan(eq(plan), isNull(), isNull(),
                any()))
                .thenReturn(SegmentRegistryResult.busy());

        assertFalse(coordinator.applySplitPlan(plan).isOk());

        verify(keyToSegmentMap, never()).applySplitPlan(plan);
        verify(keyToSegmentMap, never()).optionalyFlush();
        verify(segmentRegistry).applySplitPlan(eq(plan), isNull(), isNull(),
                any());
        verify(segmentRegistry, never()).closeSegmentInstance(removedSegment);
        verify(segmentRegistry, never())
                .deleteSegmentFiles(plan.getOldSegmentId());
    }
}
