package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;

import org.hestiastore.index.segmentregistry.SegmentRegistryResultStatus;
import org.junit.jupiter.api.Test;

class SegmentRegistryResultStatusTest {

    @Test
    void containsAllStatuses() {
        final EnumSet<SegmentRegistryResultStatus> statuses = EnumSet
                .allOf(SegmentRegistryResultStatus.class);

        assertTrue(statuses.contains(SegmentRegistryResultStatus.OK));
        assertTrue(statuses.contains(SegmentRegistryResultStatus.BUSY));
        assertTrue(statuses.contains(SegmentRegistryResultStatus.CLOSED));
        assertTrue(statuses.contains(SegmentRegistryResultStatus.ERROR));
    }
}
