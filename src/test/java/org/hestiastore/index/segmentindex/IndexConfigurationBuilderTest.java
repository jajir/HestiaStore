package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class IndexConfigurationBuilderTest {

    @Test
    void build_derivesWriteCacheDuringMaintenanceWhenMissing() {
        final IndexConfiguration<Integer, String> config = IndexConfiguration
                .<Integer, String>builder()
                .withMaxNumberOfKeysInSegmentWriteCache(10)
                .build();

        assertEquals(14,
                config.getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance());
    }

    @Test
    void build_rejectsWriteCacheDuringMaintenanceNotGreaterThanWriteCache() {
        final IndexConfigurationBuilder<Integer, String> builder = IndexConfiguration
                .<Integer, String>builder()
                .withMaxNumberOfKeysInSegmentWriteCache(10)
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(10);

        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class, builder::build);

        assertEquals(
                "Property 'maxNumberOfKeysInSegmentWriteCacheDuringMaintenance' must be greater than 'maxNumberOfKeysInSegmentWriteCache'",
                ex.getMessage());
    }
}
