package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;

import org.junit.jupiter.api.Test;

class SegmentHandlerResultStatusTest {

    @Test
    void status_containsExpectedValues() {
        final EnumSet<SegmentHandlerResultStatus> statuses = EnumSet
                .allOf(SegmentHandlerResultStatus.class);

        assertTrue(statuses.contains(SegmentHandlerResultStatus.OK));
        assertTrue(statuses.contains(SegmentHandlerResultStatus.LOCKED));
    }
}
