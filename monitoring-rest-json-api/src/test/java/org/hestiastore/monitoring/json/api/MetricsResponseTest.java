package org.hestiastore.monitoring.json.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;

import org.junit.jupiter.api.Test;

class MetricsResponseTest {

    @Test
    void keepsCountersAndNormalizesNames() {
        final Instant now = Instant.now();
        final MetricsResponse response = new MetricsResponse(
                " idx ", " READY ",
                1L, 2L, 3L,
                4L, 5L, 6L, 7L,
                8, 9, 10,
                11, 12, 13, 14,
                15, 16, 17, 18, 19, 20,
                21L, 22L, 23L, 24L,
                25L, 26L, 27L,
                28, 29, 30, 31, 32,
                33L, 34L, 35L, 36L, 37L, 38L,
                39, 40, 0.01D,
                41L, 42L, 43L, 44L,
                45L, 46L, 47L, 48L, 49L,
                now);

        assertEquals("idx", response.indexName());
        assertEquals("READY", response.state());
        assertEquals(1L, response.getOperationCount());
        assertEquals(2L, response.putOperationCount());
        assertEquals(3L, response.deleteOperationCount());
        assertEquals(0.01D, response.bloomFilterProbabilityOfFalsePositive());
        assertEquals(now, response.capturedAt());
    }

    @Test
    void rejectsNegativeCounters() {
        final Instant capturedAt = Instant.now();

        assertThrows(IllegalArgumentException.class,
                () -> new MetricsResponse("idx", "READY",
                        -1L, 0L, 0L,
                        0L, 0L, 0L, 0L,
                        0, 0, 0,
                        0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0,
                        0L, 0L, 0L, 0L,
                        0L, 0L, 0L,
                        0, 0, 0, 0, 0,
                        0L, 0L, 0L, 0L, 0L, 0L,
                        0, 0, 0D,
                        0L, 0L, 0L, 0L,
                        0L, 0L, 0L, 0L, 0L,
                        capturedAt));
    }
}
