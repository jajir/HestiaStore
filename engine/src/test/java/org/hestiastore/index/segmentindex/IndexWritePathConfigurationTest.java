package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class IndexWritePathConfigurationTest {

    @Test
    void storesCanonicalWritePathValues() {
        final IndexWritePathConfiguration configuration = new IndexWritePathConfiguration(
                10, 14, 42, 99);

        assertEquals(10, configuration.segmentWriteCacheKeyLimit());
        assertEquals(14,
                configuration.segmentWriteCacheKeyLimitDuringMaintenance());
        assertEquals(42, configuration.indexBufferedWriteKeyLimit());
        assertEquals(99, configuration.segmentSplitKeyThreshold());
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
