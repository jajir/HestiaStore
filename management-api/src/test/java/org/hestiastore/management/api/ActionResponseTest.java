package org.hestiastore.management.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;

import org.junit.jupiter.api.Test;

class ActionResponseTest {

    @Test
    void normalizesMessageAndRequestId() {
        final Instant now = Instant.now();
        final ActionResponse response = new ActionResponse(" req-1 ",
                ActionType.FLUSH, ActionStatus.ACCEPTED, null, now);
        assertEquals("req-1", response.requestId());
        assertEquals("", response.message());
        assertEquals(now, response.capturedAt());
    }

    @Test
    void rejectsBlankRequestId() {
        assertThrows(IllegalArgumentException.class,
                () -> new ActionResponse(" ", ActionType.FLUSH,
                        ActionStatus.ACCEPTED, "", Instant.now()));
    }
}
