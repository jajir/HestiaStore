package org.hestiastore.index.segment;

import org.hestiastore.index.OperationStatus;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class OperationStatusTest {

    @Test
    void valueOf_returnsExpectedValues() {
        assertEquals(OperationStatus.OK,
                OperationStatus.valueOf("OK"));
        assertEquals(OperationStatus.BUSY,
                OperationStatus.valueOf("BUSY"));
        assertEquals(OperationStatus.WRITE_CACHE_FULL,
                OperationStatus.valueOf("WRITE_CACHE_FULL"));
        assertEquals(OperationStatus.CLOSED,
                OperationStatus.valueOf("CLOSED"));
        assertEquals(OperationStatus.ERROR,
                OperationStatus.valueOf("ERROR"));
    }

    @Test
    void values_containsAllStatuses() {
        assertEquals(5, OperationStatus.values().length);
    }
}
