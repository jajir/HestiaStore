package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SegmentMaintenanceDecisionTest {

    @Test
    void noneProducesNoMaintenance() {
        final SegmentMaintenanceDecision decision = SegmentMaintenanceDecision
                .none();

        assertFalse(decision.shouldFlush());
        assertFalse(decision.shouldCompact());
    }

    @Test
    void factoryMethodsExposeExpectedFlags() {
        final SegmentMaintenanceDecision flushOnly = SegmentMaintenanceDecision
                .flushOnly();
        assertTrue(flushOnly.shouldFlush());
        assertFalse(flushOnly.shouldCompact());

        final SegmentMaintenanceDecision compactOnly = SegmentMaintenanceDecision
                .compactOnly();
        assertFalse(compactOnly.shouldFlush());
        assertTrue(compactOnly.shouldCompact());
    }
}
