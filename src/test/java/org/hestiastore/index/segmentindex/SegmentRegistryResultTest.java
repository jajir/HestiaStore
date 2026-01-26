package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SegmentRegistryResultTest {

    @Test
    void okCarriesValue() {
        final SegmentRegistryResult<String> result = SegmentRegistryResult
                .ok("value");

        assertEquals(SegmentRegistryResultStatus.OK, result.getStatus());
        assertEquals("value", result.getValue());
        assertTrue(result.isOk());
    }

    @Test
    void okWithoutValueStoresNull() {
        final SegmentRegistryResult<Void> result = SegmentRegistryResult.ok();

        assertEquals(SegmentRegistryResultStatus.OK, result.getStatus());
        assertNull(result.getValue());
        assertTrue(result.isOk());
    }

    @Test
    void busyHasNullValue() {
        final SegmentRegistryResult<String> result = SegmentRegistryResult
                .busy();

        assertEquals(SegmentRegistryResultStatus.BUSY, result.getStatus());
        assertNull(result.getValue());
        assertFalse(result.isOk());
    }

    @Test
    void closedHasNullValue() {
        final SegmentRegistryResult<String> result = SegmentRegistryResult
                .closed();

        assertEquals(SegmentRegistryResultStatus.CLOSED, result.getStatus());
        assertNull(result.getValue());
        assertFalse(result.isOk());
    }

    @Test
    void errorHasNullValue() {
        final SegmentRegistryResult<String> result = SegmentRegistryResult
                .error();

        assertEquals(SegmentRegistryResultStatus.ERROR, result.getStatus());
        assertNull(result.getValue());
        assertFalse(result.isOk());
    }
}
