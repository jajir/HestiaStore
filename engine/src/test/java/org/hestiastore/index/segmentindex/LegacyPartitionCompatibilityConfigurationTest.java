package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class LegacyPartitionCompatibilityConfigurationTest {

    @Test
    void exposesLegacyNamesFromCanonicalWritePathConfiguration() {
        final LegacyPartitionCompatibilityConfiguration configuration = new LegacyPartitionCompatibilityConfiguration(
                new IndexWritePathConfiguration(10, 14, 42, 99), 2);

        assertEquals(10, configuration.getMaxNumberOfKeysInActivePartition());
        assertEquals(2,
                configuration.getMaxNumberOfImmutableRunsPerPartition());
        assertEquals(14, configuration.getMaxNumberOfKeysInPartitionBuffer());
        assertEquals(42, configuration.getMaxNumberOfKeysInIndexBuffer());
        assertEquals(99, configuration.getMaxNumberOfKeysInPartitionBeforeSplit());
    }

    @Test
    void rejectsNegativeLegacyImmutableRunLimit() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new LegacyPartitionCompatibilityConfiguration(
                        new IndexWritePathConfiguration(10, 14, 42, 99), 0));

        assertEquals("maxNumberOfImmutableRunsPerPartition must be >= 1",
                ex.getMessage());
    }
}
