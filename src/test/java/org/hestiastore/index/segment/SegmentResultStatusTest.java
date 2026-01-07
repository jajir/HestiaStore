package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class SegmentResultStatusTest {

    @Test
    void valueOf_returnsExpectedValues() {
        assertEquals(SegmentResultStatus.OK,
                SegmentResultStatus.valueOf("OK"));
        assertEquals(SegmentResultStatus.BUSY,
                SegmentResultStatus.valueOf("BUSY"));
        assertEquals(SegmentResultStatus.CLOSED,
                SegmentResultStatus.valueOf("CLOSED"));
        assertEquals(SegmentResultStatus.ERROR,
                SegmentResultStatus.valueOf("ERROR"));
    }

    @Test
    void values_containsAllStatuses() {
        assertEquals(4, SegmentResultStatus.values().length);
    }
}
