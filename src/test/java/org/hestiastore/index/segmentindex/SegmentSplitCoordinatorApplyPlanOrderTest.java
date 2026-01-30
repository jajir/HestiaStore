package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentregistry.SegmentRegistryFreeze;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentSplitCoordinatorApplyPlanOrderTest {

    @Mock
    private KeyToSegmentMapSynchronizedAdapter<Integer> keyToSegmentMap;

    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;

    @Mock
    private SegmentRegistryAccess<Integer, String> registryAccess;

    @Mock
    private Segment<Integer, String> segment;

    private SegmentWriterTxFactory<Integer, String> writerTxFactory;

    private SegmentSplitCoordinator<Integer, String> coordinator;

    @BeforeEach
    void setUp() {
        writerTxFactory = id -> {
            throw new IllegalStateException("writerTxFactory not configured");
        };
        coordinator = new SegmentSplitCoordinator<>(
                IndexConfiguration.<Integer, String>builder().build(),
                keyToSegmentMap, segmentRegistry, registryAccess,
                writerTxFactory);
    }

    @AfterEach
    void tearDown() {
        coordinator = null;
        writerTxFactory = null;
    }

    @Test
    void applySplitPlan_persists_map_after_apply() {
        final SegmentSplitApplyPlan<Integer, String> plan = new SegmentSplitApplyPlan<>(
                SegmentId.of(1), SegmentId.of(2), SegmentId.of(3), 1, 10,
                SegmentSplitterResult.SegmentSplittingStatus.SPLIT);
        final SegmentRegistryFreeze freeze = () -> {
        };
        when(registryAccess.tryEnterFreeze())
                .thenReturn(SegmentRegistryResult.ok(freeze));
        when(keyToSegmentMap.applySplitPlan(plan)).thenReturn(true);
        when(registryAccess.evictSegmentFromCache(plan.getOldSegmentId(),
                segment)).thenReturn(SegmentRegistryResult.ok());

        assertTrue(coordinator.applySplitPlan(plan, segment).isOk());

        verify(keyToSegmentMap).applySplitPlan(plan);
        verify(keyToSegmentMap).optionalyFlush();
        verify(registryAccess).evictSegmentFromCache(plan.getOldSegmentId(),
                segment);
    }

    @Test
    void applySplitPlan_skips_map_flush_when_apply_fails() {
        final SegmentSplitApplyPlan<Integer, String> plan = new SegmentSplitApplyPlan<>(
                SegmentId.of(1), SegmentId.of(2), SegmentId.of(3), 1, 10,
                SegmentSplitterResult.SegmentSplittingStatus.SPLIT);
        final SegmentRegistryFreeze freeze = () -> {
        };
        when(registryAccess.tryEnterFreeze())
                .thenReturn(SegmentRegistryResult.ok(freeze));
        when(keyToSegmentMap.applySplitPlan(plan)).thenReturn(false);

        assertFalse(coordinator.applySplitPlan(plan, segment).isOk());

        verify(keyToSegmentMap, never()).optionalyFlush();
        verify(registryAccess).failRegistry();
        verify(registryAccess, never()).evictSegmentFromCache(
                plan.getOldSegmentId(), segment);
    }
}
