package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class IndexConfigurationRegistryTest {

    @Test
    void defaultsAreRegisteredForKnownTypes() {
        assertTrue(IndexConfigurationRegistry.get(Integer.class).isPresent());
        assertTrue(IndexConfigurationRegistry.get(Long.class).isPresent());
        assertTrue(IndexConfigurationRegistry.get(String.class).isPresent());
    }

    @Test
    void addStoresCustomConfigurationForKeyAndMemory() {
        final IndexConfigurationContract custom = new IndexConfigurationContract() {
        };
        IndexConfigurationRegistry.add(String.class, "heap", custom);

        assertSame(custom, IndexConfigurationRegistry.get(String.class, "heap")
                .orElseThrow());
    }
}
