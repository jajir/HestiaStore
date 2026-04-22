package org.hestiastore.index.segment;

import org.hestiastore.index.OperationStatus;
import org.hestiastore.index.OperationResult;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SegmentResultTest {

    @Test
    void ok_withValue_setsStatusAndValue() {
        final OperationResult<String> result = OperationResult.ok("value");

        assertEquals(OperationStatus.OK, result.getStatus());
        assertEquals("value", result.getValue());
        assertTrue(result.isOk());
    }

    @Test
    void ok_withoutValue_setsStatusAndNullValue() {
        final OperationResult<Void> result = OperationResult.ok();

        assertEquals(OperationStatus.OK, result.getStatus());
        assertNull(result.getValue());
        assertTrue(result.isOk());
    }

    @Test
    void nonOk_results_haveNullValue_and_areNotOk() {
        final OperationResult<String> busy = OperationResult.busy();
        final OperationResult<String> closed = OperationResult.closed();
        final OperationResult<String> error = OperationResult.error();

        assertEquals(OperationStatus.BUSY, busy.getStatus());
        assertNull(busy.getValue());
        assertFalse(busy.isOk());

        assertEquals(OperationStatus.CLOSED, closed.getStatus());
        assertNull(closed.getValue());
        assertFalse(closed.isOk());

        assertEquals(OperationStatus.ERROR, error.getStatus());
        assertNull(error.getValue());
        assertFalse(error.isOk());
    }
}
