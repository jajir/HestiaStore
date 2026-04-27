package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class IndexWritePathConfigurationTest {

    @Test
    void storesCanonicalWritePathValues() {
        final IndexWritePathConfiguration configuration = new IndexWritePathConfiguration(
                10, 14, 42, 99);

        assertEquals(10, configuration.getSegmentWriteCacheKeyLimit());
        assertEquals(14,
                configuration.getSegmentWriteCacheKeyLimitDuringMaintenance());
        assertEquals(42, configuration.getIndexBufferedWriteKeyLimit());
        assertEquals(99, configuration.getSegmentSplitKeyThreshold());
    }

    @Test
    void rejectsMaintenanceLimitNotGreaterThanWriteCacheLimit() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new IndexWritePathConfiguration(10, 10, 42, 99));

        assertEquals(
                "segmentWriteCacheKeyLimitDuringMaintenance must be greater than segmentWriteCacheKeyLimit",
                ex.getMessage());
    }
}
