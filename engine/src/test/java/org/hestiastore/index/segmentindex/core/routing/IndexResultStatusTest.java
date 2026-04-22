package org.hestiastore.index.segmentindex.core.routing;

import org.hestiastore.index.OperationStatus;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;

import org.junit.jupiter.api.Test;

class OperationStatusTest {

    @Test
    void containsExpectedStatuses() {
        final EnumSet<OperationStatus> statuses = EnumSet
                .allOf(OperationStatus.class);

        assertTrue(statuses.contains(OperationStatus.OK));
        assertTrue(statuses.contains(OperationStatus.BUSY));
        assertTrue(statuses.contains(OperationStatus.CLOSED));
        assertTrue(statuses.contains(OperationStatus.ERROR));
        assertEquals(4, statuses.size());
    }
}
