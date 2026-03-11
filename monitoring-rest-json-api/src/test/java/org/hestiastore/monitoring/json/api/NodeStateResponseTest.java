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
                11, 12, 13, 14,
                15, 16, 17, 18, 19, 20,
                21L, 22L, 23L, 24L,
                25L, 26L, 27L,
                28, 29, 30, 31, 32,
                33L, 34L, 35L, 36L, 37L, 38L,
                39, 40, 0.01D,
                41L, 42L, 43L, 44L);
        final NodeReportResponse response = new NodeReportResponse(jvm,
                List.of(index), now);
        assertEquals(jvm, response.jvm());
        assertEquals(1, response.indexes().size());
        assertEquals(now, response.capturedAt());
    }

    @Test
    void rejectsNullJvmSection() {
        final List<IndexReportResponse> indexes = List.of();
        final Instant now = Instant.now();
        assertThrows(NullPointerException.class,
                () -> new NodeReportResponse(null, indexes, now));
    }
}
