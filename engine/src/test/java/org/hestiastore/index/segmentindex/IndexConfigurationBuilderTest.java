package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class IndexConfigurationBuilderTest {

    @Test
    void build_derivesPartitionBufferWhenMissing() {
        final IndexConfiguration<Integer, String> config = IndexConfiguration
                .<Integer, String>builder()
                .withMaxNumberOfKeysInActivePartition(10)
                .build();

        assertEquals(14,
                config.getMaxNumberOfKeysInPartitionBuffer());
    }

    @Test
    void build_keepsCanonicalPartitionSettingsInSync() {
        final IndexConfiguration<Integer, String> config = IndexConfiguration
                .<Integer, String>builder()
                .withMaxNumberOfKeysInActivePartition(10)
                .withMaxNumberOfKeysInPartitionBuffer(14)
                .build();

        assertEquals(10, config.getMaxNumberOfKeysInActivePartition());
        assertEquals(14, config.getMaxNumberOfKeysInPartitionBuffer());
    }

    @Test
    void build_rejectsPartitionBufferNotGreaterThanActivePartition() {
        final IndexConfigurationBuilder<Integer, String> builder = IndexConfiguration
                .<Integer, String>builder()
                .withMaxNumberOfKeysInActivePartition(10)
                .withMaxNumberOfKeysInPartitionBuffer(10);

        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class, builder::build);

        assertEquals(
                "Property 'maxNumberOfKeysInPartitionBuffer' must be greater than 'maxNumberOfKeysInActivePartition'",
                ex.getMessage());
    }
}
