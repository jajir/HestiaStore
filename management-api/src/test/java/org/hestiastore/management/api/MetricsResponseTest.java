package org.hestiastore.management.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;

import org.junit.jupiter.api.Test;

class MetricsResponseTest {

    @Test
    void keepsCountersAndNormalizesNames() {
        final Instant now = Instant.now();
        final MetricsResponse response = new MetricsResponse(" idx ", " READY ",
                1L, 2L, 3L, now);
        assertEquals("idx", response.indexName());
        assertEquals("READY", response.state());
        assertEquals(1L, response.getOperationCount());
        assertEquals(2L, response.putOperationCount());
        assertEquals(3L, response.deleteOperationCount());
        assertEquals(0L, response.registryCacheHitCount());
        assertEquals(0L, response.registryCacheMissCount());
        assertEquals(0L, response.registryCacheLoadCount());
        assertEquals(0L, response.registryCacheEvictionCount());
        assertEquals(0, response.registryCacheSize());
        assertEquals(0, response.registryCacheLimit());
        assertEquals(now, response.capturedAt());
    }

    @Test
    void rejectsNegativeCounters() {
        assertThrows(IllegalArgumentException.class,
                () -> new MetricsResponse("idx", "READY", -1L, 0L, 0L,
                        Instant.now()));
    }
}
