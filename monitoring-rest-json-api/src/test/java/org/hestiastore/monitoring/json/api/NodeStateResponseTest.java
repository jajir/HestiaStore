package org.hestiastore.monitoring.json.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

class NodeStateResponseTest {

    @Test
    void storesJvmAndIndexSections() {
        final Instant now = Instant.now();
        final JvmMetricsResponse jvm = new JvmMetricsResponse(1L, 2L, 3L, 4L,
                5L, 6L);
        final IndexReportResponse index = new IndexReportResponse("idx",
                "READY", true,
                1L, 2L, 3L,
                4L, 5L, 6L, 7L,
                8, 9, 10,
                11, 12,
                13, 14, 15, 16, 17, 18,
                19L, 20L, 21L, 22L,
                23L, 24L, 25L,
                26, 27, 28, 29, 30,
                31L, 32L, 33L, 34L, 35L, 36L,
                37, 38, 0.01D,
                39L, 40L, 41L, 42L);
        final NodeReportResponse response = new NodeReportResponse(jvm,
                List.of(index), now);
        assertEquals(jvm, response.jvm());
        assertEquals(1, response.indexes().size());
        assertEquals(now, response.capturedAt());
    }

    @Test
    void rejectsNullJvmSection() {
        assertThrows(NullPointerException.class,
                () -> new NodeReportResponse(null, List.of(), Instant.now()));
    }
}
