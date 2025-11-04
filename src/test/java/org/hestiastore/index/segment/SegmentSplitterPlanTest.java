package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import org.hestiastore.index.Entry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentSplitterPlanTest {

    @Mock
    private SegmentPropertiesManager segmentPropertiesManager;

    @Mock
    private SegmentDeltaCacheController<String, String> deltaCacheController;

    @Mock
    private SegmentStats segmentStats;

    private SegmentSplitterPlan<String, String> newPlan() {
        when(segmentPropertiesManager.getSegmentStats()).thenReturn(segmentStats);
        when(segmentStats.getNumberOfKeysInSegment()).thenReturn(2L);
        when(deltaCacheController.getDeltaCacheSizeWithoutTombstones()).thenReturn(1);
        final SegmentSplitterPolicy<String, String> policy = new SegmentSplitterPolicy<>(
                segmentPropertiesManager, deltaCacheController);
        return SegmentSplitterPlan.fromPolicy(policy);
    }

    @Test
    void isLowerSegmentEmpty_initially_true() {
        final SegmentSplitterPlan<String, String> plan = newPlan();
        assertTrue(plan.isLowerSegmentEmpty());
    }

    @Test
    void isLowerSegmentEmpty_after_recordLower_false() {
        final SegmentSplitterPlan<String, String> plan = newPlan();
        plan.recordLower(Entry.of("k1", "v1"));
        assertFalse(plan.isLowerSegmentEmpty());
    }
}

