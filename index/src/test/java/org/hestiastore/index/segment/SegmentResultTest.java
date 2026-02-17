package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SegmentResultTest {

    @Test
    void ok_withValue_setsStatusAndValue() {
        final SegmentResult<String> result = SegmentResult.ok("value");

        assertEquals(SegmentResultStatus.OK, result.getStatus());
        assertEquals("value", result.getValue());
        assertTrue(result.isOk());
    }

    @Test
    void ok_withoutValue_setsStatusAndNullValue() {
        final SegmentResult<Void> result = SegmentResult.ok();

        assertEquals(SegmentResultStatus.OK, result.getStatus());
        assertNull(result.getValue());
        assertTrue(result.isOk());
    }

    @Test
    void nonOk_results_haveNullValue_and_areNotOk() {
        final SegmentResult<String> busy = SegmentResult.busy();
        final SegmentResult<String> closed = SegmentResult.closed();
        final SegmentResult<String> error = SegmentResult.error();

        assertEquals(SegmentResultStatus.BUSY, busy.getStatus());
        assertNull(busy.getValue());
        assertFalse(busy.isOk());

        assertEquals(SegmentResultStatus.CLOSED, closed.getStatus());
        assertNull(closed.getValue());
        assertFalse(closed.isOk());

        assertEquals(SegmentResultStatus.ERROR, error.getStatus());
        assertNull(error.getValue());
        assertFalse(error.isOk());
    }
}
