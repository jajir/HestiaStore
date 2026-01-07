package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class IndexConfigurationDefaultIntegerTest {

    @Test
    void exposesIntegerDefaults() {
        final IndexConfigurationDefaultInteger defaults = new IndexConfigurationDefaultInteger();

        assertEquals(500_000, defaults.getMaxNumberOfKeysInSegmentCache());
        assertEquals(1_000, defaults.getMaxNumberOfKeysInSegmentChunk());
        assertEquals(5_000_000, defaults.getMaxNumberOfKeysInCache());
        assertEquals(10_000_000, defaults.getMaxNumberOfKeysInSegment());
        assertEquals(10, defaults.getMaxNumberOfSegmentsInCache());
        assertEquals(1024 * 1024, defaults.getDiskIoBufferSizeInBytes());
        assertEquals(2, defaults.getBloomFilterNumberOfHashFunctions());
        assertEquals(100_000, defaults.getBloomFilterIndexSizeInBytes());
    }
}
