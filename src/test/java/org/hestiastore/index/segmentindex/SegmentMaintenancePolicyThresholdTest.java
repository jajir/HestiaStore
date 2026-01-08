package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentStats;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentMaintenancePolicyThresholdTest {

    @Mock
    private Segment<Integer, String> segment;

    @BeforeEach
    void setUp() {
        when(segment.getNumberOfKeysInWriteCache()).thenReturn(0);
        when(segment.getStats()).thenReturn(new SegmentStats(0, 0, 0));
    }

    @Test
    void flush_only_when_write_cache_threshold_reached() {
        when(segment.getNumberOfKeysInWriteCache()).thenReturn(5);
        when(segment.getStats()).thenReturn(new SegmentStats(1, 0, 0));
        final SegmentMaintenancePolicyThreshold<Integer, String> policy = new SegmentMaintenancePolicyThreshold<>(
                5, 10);

        final SegmentMaintenanceDecision decision = policy
                .evaluateAfterWrite(segment);

        assertTrue(decision.shouldFlush());
        assertFalse(decision.shouldCompact());
    }

    @Test
    void compact_only_when_delta_cache_threshold_reached() {
        when(segment.getNumberOfKeysInWriteCache()).thenReturn(1);
        when(segment.getStats()).thenReturn(new SegmentStats(5, 0, 0));
        final SegmentMaintenancePolicyThreshold<Integer, String> policy = new SegmentMaintenancePolicyThreshold<>(
                10, 5);

        final SegmentMaintenanceDecision decision = policy
                .evaluateAfterWrite(segment);

        assertFalse(decision.shouldFlush());
        assertTrue(decision.shouldCompact());
    }

    @Test
    void flush_and_compact_when_both_thresholds_reached() {
        when(segment.getNumberOfKeysInWriteCache()).thenReturn(5);
        when(segment.getStats()).thenReturn(new SegmentStats(5, 0, 0));
        final SegmentMaintenancePolicyThreshold<Integer, String> policy = new SegmentMaintenancePolicyThreshold<>(
                5, 5);

        final SegmentMaintenanceDecision decision = policy
                .evaluateAfterWrite(segment);

        assertTrue(decision.shouldFlush());
        assertTrue(decision.shouldCompact());
    }
}
