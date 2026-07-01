package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;

class SegmentMaintenancePolicyTest {

    @Test
    void nonePolicyNeverSchedulesMaintenance() {
        @SuppressWarnings("unchecked")
        final Segment<Integer, String> segment = (Segment<Integer, String>) mock(
                Segment.class);

        final SegmentMaintenanceDecision decision = SegmentMaintenancePolicy
                .<Integer, String>none().evaluateAfterWrite(segment);

        assertFalse(decision.shouldFlush());
        assertFalse(decision.shouldCompact());
    }
}
