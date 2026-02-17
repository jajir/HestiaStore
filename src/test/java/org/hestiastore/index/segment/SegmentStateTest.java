package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class SegmentStateTest {

    @Test
    void valueOf_returnsExpectedValues() {
        assertEquals(SegmentState.READY, SegmentState.valueOf("READY"));
        assertEquals(SegmentState.FREEZE, SegmentState.valueOf("FREEZE"));
        assertEquals(SegmentState.MAINTENANCE_RUNNING,
                SegmentState.valueOf("MAINTENANCE_RUNNING"));
        assertEquals(SegmentState.CLOSED, SegmentState.valueOf("CLOSED"));
        assertEquals(SegmentState.ERROR, SegmentState.valueOf("ERROR"));
    }

    @Test
    void values_containsAllStates() {
        assertEquals(5, SegmentState.values().length);
    }
}
