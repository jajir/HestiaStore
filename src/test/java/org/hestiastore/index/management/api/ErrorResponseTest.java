package org.hestiastore.index.management.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;

import org.junit.jupiter.api.Test;

class ErrorResponseTest {

    @Test
    void normalizesOptionalRequestId() {
        final Instant now = Instant.now();
        final ErrorResponse response = new ErrorResponse("INVALID_STATE",
                "Operation is not allowed.", null, now);
        assertEquals("", response.requestId());
        assertEquals(now, response.capturedAt());
    }

    @Test
    void rejectsBlankCode() {
        assertThrows(IllegalArgumentException.class,
                () -> new ErrorResponse(" ", "x", "", Instant.now()));
    }
}
