package org.hestiastore.management.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;

import org.junit.jupiter.api.Test;

class NodeStateResponseTest {

    @Test
    void normalizesStateAndName() {
        final Instant now = Instant.now();
        final NodeStateResponse response = new NodeStateResponse(" idx ",
                " READY ", true, now);
        assertEquals("idx", response.indexName());
        assertEquals("READY", response.state());
        assertEquals(true, response.ready());
        assertEquals(now, response.capturedAt());
    }

    @Test
    void rejectsBlankState() {
        assertThrows(IllegalArgumentException.class,
                () -> new NodeStateResponse("idx", " ", true, Instant.now()));
    }
}
