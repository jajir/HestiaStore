package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class IndexConfigurationDefaultStringTest {

    @Test
    void exposesStringDefaults() {
        final IndexConfigurationDefaultString defaults = new IndexConfigurationDefaultString();

        assertEquals(500_000, defaults.getMaxNumberOfKeysInSegmentCache());
        assertEquals(10_000, defaults.getMaxNumberOfKeysInSegmentChunk());
        assertEquals(10_000_000, defaults.getMaxNumberOfKeysInSegment());
        assertEquals(10, defaults.getMaxNumberOfSegmentsInCache());
        assertEquals(IndexConfigurationContract.MAX_NUMBER_OF_DELTA_CACHE_FILES,
                defaults.getMaxNumberOfDeltaCacheFiles());
        assertEquals(1024 * 1024, defaults.getDiskIoBufferSizeInBytes());
        assertEquals(2, defaults.getBloomFilterNumberOfHashFunctions());
        assertEquals(1_000_000, defaults.getBloomFilterIndexSizeInBytes());
        assertEquals(3, defaults.getNumberOfRegistryLifecycleThreads());
    }
}
