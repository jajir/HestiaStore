package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;

import org.hestiastore.index.segmentregistry.SegmentRegistryState;
import org.junit.jupiter.api.Test;

class SegmentRegistryStateTest {

    @Test
    void exposes_expected_states() {
        final EnumSet<SegmentRegistryState> states = EnumSet
                .allOf(SegmentRegistryState.class);
        assertTrue(states.contains(SegmentRegistryState.READY));
        assertTrue(states.contains(SegmentRegistryState.FREEZE));
        assertTrue(states.contains(SegmentRegistryState.CLOSED));
        assertTrue(states.contains(SegmentRegistryState.ERROR));
    }
}
