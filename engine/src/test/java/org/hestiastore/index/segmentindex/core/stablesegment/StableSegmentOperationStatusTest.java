package org.hestiastore.index.segmentindex.core.stablesegment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;

import org.junit.jupiter.api.Test;

class StableSegmentOperationStatusTest {

    @Test
    void containsExpectedStatuses() {
        final EnumSet<StableSegmentOperationStatus> statuses = EnumSet
                .allOf(StableSegmentOperationStatus.class);

        assertTrue(statuses.contains(StableSegmentOperationStatus.OK));
        assertTrue(statuses.contains(StableSegmentOperationStatus.BUSY));
        assertTrue(statuses.contains(StableSegmentOperationStatus.CLOSED));
        assertTrue(statuses.contains(StableSegmentOperationStatus.ERROR));
        assertEquals(4, statuses.size());
    }
}
