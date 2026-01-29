package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;

import org.junit.jupiter.api.Test;

class SegmentHandlerStateTest {

    @Test
    void state_containsExpectedValues() {
        final EnumSet<SegmentHandlerState> states = EnumSet
                .allOf(SegmentHandlerState.class);

        assertTrue(states.contains(SegmentHandlerState.READY));
        assertTrue(states.contains(SegmentHandlerState.LOCKED));
    }
}
