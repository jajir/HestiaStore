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
        final IndexReportResponse index = indexReport();
        final NodeReportResponse response = new NodeReportResponse(jvm,
                List.of(index), now);
        assertEquals(jvm, response.jvm());
        assertEquals(1, response.indexes().size());
        assertEquals("idx", response.indexes().get(0).indexName());
        assertEquals(now, response.capturedAt());
    }

    @Test
    void rejectsNullJvmSection() {
        final List<IndexReportResponse> indexes = List.of();
        final Instant now = Instant.now();
        assertThrows(NullPointerException.class,
                () -> new NodeReportResponse(null, indexes, now));
    }

    private IndexReportResponse indexReport() {
        final ExecutorReportResponse executor = new ExecutorReportResponse(1, 2,
                3, 4L, 5L, 6L);
        return new IndexReportResponse("idx", "READY", true,
                new OperationReportResponse(1L, 2L, 3L),
                new RegistryCacheReportResponse(4L, 5L, 6L, 7L, 8, 9),
                new ChunkStoreCacheReportResponse(10, 11, 12L, 13L, 14L, 15L,
                        16L, 17L),
                new SegmentReportResponse(18, 19, 20, 21, 22, 23, 24, 25L,
                        26L, 27L, List.of()),
                new WritePathReportResponse(28, 29, 30, 31L),
                new MaintenanceReportResponse(35L, 36L, 37L, 38L, 39L, 40L,
                        executor, executor),
                new SplitReportResponse(41L, 42, 43, 44L, 45L, executor),
                new LatencyReportResponse(46L, 47L, 48L, 49L, 50L, 51L),
                new BloomFilterReportResponse(52, 53, 0.01D, 54L, 55L, 56L,
                        57L),
                new WalReportResponse(true, 58L, 59L, 60L, 61L, 62L, 63L, 64L,
                        65, 66L, 67L, 68L, 69L, 70L, 71L, 72L, 73L, 74L, 75L,
                        76L));
    }
}
