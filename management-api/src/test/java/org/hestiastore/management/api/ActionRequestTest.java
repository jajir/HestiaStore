package org.hestiastore.management.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class ActionRequestTest {

    @Test
    void trimsRequestId() {
        final ActionRequest request = new ActionRequest("  req-1 ");
        assertEquals("req-1", request.requestId());
    }

    @Test
    void rejectsBlankRequestId() {
        assertThrows(IllegalArgumentException.class,
                () -> new ActionRequest("   "));
    }
}
