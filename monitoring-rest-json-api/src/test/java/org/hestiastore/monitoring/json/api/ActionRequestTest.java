package org.hestiastore.monitoring.json.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class ActionRequestTest {

    @Test
    void trimsRequestIdAndIndexName() {
        final ActionRequest request = new ActionRequest("  req-1 ",
                " orders ");
        assertEquals("req-1", request.requestId());
        assertEquals("orders", request.indexName());
    }

    @Test
    void normalizesBlankIndexNameToNull() {
        final ActionRequest request = new ActionRequest("req-1", "  ");
        assertNull(request.indexName());
    }

    @Test
    void rejectsBlankRequestId() {
        assertThrows(IllegalArgumentException.class,
                () -> new ActionRequest("   "));
    }
}
