package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class IndexResultTest {

    @Test
    void okCarriesValue() {
        final IndexResult<String> result = IndexResult.ok("value");

        assertEquals(IndexResultStatus.OK, result.getStatus());
        assertEquals("value", result.getValue());
    }

    @Test
    void okWithoutValueStoresNull() {
        final IndexResult<Void> result = IndexResult.ok();

        assertEquals(IndexResultStatus.OK, result.getStatus());
        assertNull(result.getValue());
    }

    @Test
    void busyHasNullValue() {
        final IndexResult<String> result = IndexResult.busy();

        assertEquals(IndexResultStatus.BUSY, result.getStatus());
        assertNull(result.getValue());
    }

    @Test
    void closedHasNullValue() {
        final IndexResult<String> result = IndexResult.closed();

        assertEquals(IndexResultStatus.CLOSED, result.getStatus());
        assertNull(result.getValue());
    }

    @Test
    void errorHasNullValue() {
        final IndexResult<String> result = IndexResult.error();

        assertEquals(IndexResultStatus.ERROR, result.getStatus());
        assertNull(result.getValue());
    }
}
