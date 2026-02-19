package org.hestiastore.management.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class MetricsResponseTest {

    @Test
    void keepsCountersAndNormalizesNames() {
        final IndexReportResponse response = new IndexReportResponse(
                " idx ", " READY ", true,
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

        assertEquals("idx", response.indexName());
        assertEquals("READY", response.state());
        assertEquals(1L, response.getOperationCount());
        assertEquals(2L, response.putOperationCount());
        assertEquals(3L, response.deleteOperationCount());
        assertEquals(0.01D, response.bloomFilterProbabilityOfFalsePositive());
    }

    @Test
    void rejectsNegativeCounters() {
        assertThrows(IllegalArgumentException.class,
                () -> new IndexReportResponse("idx", "READY", true,
                        -1L, 0L, 0L,
                        0L, 0L, 0L, 0L,
                        0, 0, 0,
                        0, 0,
                        0, 0, 0, 0, 0, 0,
                        0L, 0L, 0L, 0L,
                        0L, 0L, 0L,
                        0, 0, 0, 0, 0,
                        0L, 0L, 0L, 0L, 0L, 0L,
                        0, 0, 0D,
                        0L, 0L, 0L, 0L));
    }
}
