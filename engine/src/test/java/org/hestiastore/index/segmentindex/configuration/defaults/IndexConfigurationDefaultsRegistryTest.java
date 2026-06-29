package org.hestiastore.index.segmentindex.configuration.defaults;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.segmentindex.configuration.api.IndexConfigurationDefaults;
import org.junit.jupiter.api.Test;

class IndexConfigurationDefaultsRegistryTest {

    @Test
    void defaultsAreRegisteredForKnownTypes() {
        assertTrue(IndexConfigurationDefaultsRegistry.get(Integer.class).isPresent());
        assertTrue(IndexConfigurationDefaultsRegistry.get(Long.class).isPresent());
        assertTrue(IndexConfigurationDefaultsRegistry.get(String.class).isPresent());
    }

    @Test
    void addStoresCustomConfigurationForKeyAndMemory() {
        final IndexConfigurationDefaults custom = new IndexConfigurationDefaults() {
        };
        IndexConfigurationDefaultsRegistry.add(String.class, "heap", custom);

        assertSame(custom, IndexConfigurationDefaultsRegistry.get(String.class, "heap")
                .orElseThrow());
    }
}
