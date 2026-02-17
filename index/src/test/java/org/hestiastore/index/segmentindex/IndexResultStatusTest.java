package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;

import org.junit.jupiter.api.Test;

class IndexResultStatusTest {

    @Test
    void containsExpectedStatuses() {
        final EnumSet<IndexResultStatus> statuses = EnumSet
                .allOf(IndexResultStatus.class);

        assertTrue(statuses.contains(IndexResultStatus.OK));
        assertTrue(statuses.contains(IndexResultStatus.BUSY));
        assertTrue(statuses.contains(IndexResultStatus.CLOSED));
        assertTrue(statuses.contains(IndexResultStatus.ERROR));
        assertEquals(4, statuses.size());
    }
}
