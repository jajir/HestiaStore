package org.hestiastore.index.segmentindex.configuration.defaults;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.hestiastore.index.segmentindex.configuration.api.IndexConfigurationDefaults;
import org.junit.jupiter.api.Test;

class StringIndexConfigurationDefaultsTest {

    @Test
    void exposesStringDefaults() {
        final StringIndexConfigurationDefaults defaults = new StringIndexConfigurationDefaults();

        assertEquals(500_000, defaults.segment().cacheKeyLimit());
        assertEquals(10_000, defaults.segment().chunkKeyLimit());
        assertEquals(10_000_000, defaults.segment().maxKeys());
        assertEquals(10, defaults.segment().cachedSegmentLimit());
        assertEquals(IndexConfigurationDefaults.DEFAULT_DELTA_CACHE_FILE_LIMIT,
                defaults.segment().deltaCacheFileLimit());
        assertEquals(1024 * 1024, defaults.io().diskBufferSizeBytes());
        assertEquals(2, defaults.bloomFilter().hashFunctions());
        assertEquals(1_000_000, defaults.bloomFilter().indexSizeBytes());
        assertEquals(3, defaults.maintenance().registryLifecycleThreads());
    }
}
