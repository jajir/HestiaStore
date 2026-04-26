package org.hestiastore.index.segmentindex.core.stablesegment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class StableSegmentOperationResultTest {

    @Test
    void okCarriesValue() {
        final StableSegmentOperationResult<String> result =
                StableSegmentOperationResult.ok("value");

        assertEquals(StableSegmentOperationStatus.OK, result.getStatus());
        assertEquals("value", result.getValue());
    }

    @Test
    void okWithoutValueStoresNull() {
        final StableSegmentOperationResult<Void> result = StableSegmentOperationResult.ok();

        assertEquals(StableSegmentOperationStatus.OK, result.getStatus());
        assertNull(result.getValue());
    }

    @Test
    void busyHasNullValue() {
        final StableSegmentOperationResult<String> result = StableSegmentOperationResult.busy();

        assertEquals(StableSegmentOperationStatus.BUSY, result.getStatus());
        assertNull(result.getValue());
    }

    @Test
    void closedHasNullValue() {
        final StableSegmentOperationResult<String> result = StableSegmentOperationResult.closed();

        assertEquals(StableSegmentOperationStatus.CLOSED, result.getStatus());
        assertNull(result.getValue());
    }

    @Test
    void errorHasNullValue() {
        final StableSegmentOperationResult<String> result = StableSegmentOperationResult.error();

        assertEquals(StableSegmentOperationStatus.ERROR, result.getStatus());
        assertNull(result.getValue());
    }
}
