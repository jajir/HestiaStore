package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SegmentHandlerResultTest {

    @Test
    void ok_returnsValueAndStatus() {
        final SegmentHandlerResult<String> result = SegmentHandlerResult
                .ok("value");

        assertEquals(SegmentHandlerResultStatus.OK, result.getStatus());
        assertEquals("value", result.getValue());
        assertTrue(result.isOk());
    }

    @Test
    void locked_returnsLockedStatus() {
        final SegmentHandlerResult<String> result = SegmentHandlerResult
                .locked();

        assertEquals(SegmentHandlerResultStatus.LOCKED, result.getStatus());
        assertNull(result.getValue());
        assertFalse(result.isOk());
    }
}
