package org.hestiastore.index.segmentindex.core.routing;

import org.hestiastore.index.OperationStatus;
import org.hestiastore.index.OperationResult;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class IndexResultTest {

    @Test
    void okCarriesValue() {
        final OperationResult<String> result = OperationResult.ok("value");

        assertEquals(OperationStatus.OK, result.getStatus());
        assertEquals("value", result.getValue());
    }

    @Test
    void okWithoutValueStoresNull() {
        final OperationResult<Void> result = OperationResult.ok();

        assertEquals(OperationStatus.OK, result.getStatus());
        assertNull(result.getValue());
    }

    @Test
    void busyHasNullValue() {
        final OperationResult<String> result = OperationResult.busy();

        assertEquals(OperationStatus.BUSY, result.getStatus());
        assertNull(result.getValue());
    }

    @Test
    void closedHasNullValue() {
        final OperationResult<String> result = OperationResult.closed();

        assertEquals(OperationStatus.CLOSED, result.getStatus());
        assertNull(result.getValue());
    }

    @Test
    void errorHasNullValue() {
        final OperationResult<String> result = OperationResult.error();

        assertEquals(OperationStatus.ERROR, result.getStatus());
        assertNull(result.getValue());
    }
}
