package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class IndexConfigurationBuilderTest {

    @Test
    void build_derivesMaintenanceWriteCacheLimitWhenMissing() {
        final IndexConfiguration<Integer, String> config = IndexConfiguration
                .<Integer, String>builder()
                .withSegmentWriteCacheKeyLimit(10)
                .build();

        assertEquals(14,
                config.getSegmentWriteCacheKeyLimitDuringMaintenance());
    }

    @Test
    void build_exposesCanonicalWritePathConfiguration() {
        final IndexConfiguration<Integer, String> config = IndexConfiguration
                .<Integer, String>builder()
                .withSegmentWriteCacheKeyLimit(10)
                .withSegmentWriteCacheKeyLimitDuringMaintenance(14)
                .withIndexBufferedWriteKeyLimit(42)
                .withSegmentSplitKeyThreshold(99)
                .build();

        assertEquals(10, config.getSegmentWriteCacheKeyLimit());
        assertEquals(14, config.getSegmentWriteCacheKeyLimitDuringMaintenance());
        assertEquals(42, config.getIndexBufferedWriteKeyLimit());
        assertEquals(99, config.getSegmentSplitKeyThreshold());
        assertEquals(10,
                config.getLegacyPartitionCompatibilityConfiguration()
                        .getMaxNumberOfKeysInActivePartition());
    }

    @Test
    void build_rejectsMaintenanceWriteCacheNotGreaterThanSegmentWriteCache() {
        final IndexConfigurationBuilder<Integer, String> builder = IndexConfiguration
                .<Integer, String>builder()
                .withSegmentWriteCacheKeyLimit(10)
                .withSegmentWriteCacheKeyLimitDuringMaintenance(10);

        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class, builder::build);

        assertEquals(
                "Property 'maxNumberOfKeysInPartitionBuffer' must be greater than 'maxNumberOfKeysInActivePartition'",
                ex.getMessage());
    }
}
