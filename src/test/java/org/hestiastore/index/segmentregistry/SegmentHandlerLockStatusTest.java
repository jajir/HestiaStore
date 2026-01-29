package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;

import org.junit.jupiter.api.Test;

class SegmentHandlerLockStatusTest {

    @Test
    void status_containsExpectedValues() {
        final EnumSet<SegmentHandlerLockStatus> statuses = EnumSet
                .allOf(SegmentHandlerLockStatus.class);

        assertTrue(statuses.contains(SegmentHandlerLockStatus.OK));
        assertTrue(statuses.contains(SegmentHandlerLockStatus.BUSY));
    }
}
