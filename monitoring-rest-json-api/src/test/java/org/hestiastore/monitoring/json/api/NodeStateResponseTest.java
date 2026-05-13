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
                13L, 14L, 15L, 16L, 17L, 18L,
                19, 20, 21, 22,
                23, 24, 25, 26, 27,
                28L, 29L, 30L, 31L,
                32L, 33L, 34L,
                35, 36, 37, 38, 39,
                40L, 41L, 42L, 43L, 44L, 45L,
                46, 47, 0.01D,
                48L, 49L, 50L, 51L, List.of());
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
