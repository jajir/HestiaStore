package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.junit.jupiter.api.Test;

class SegmentConfTest {

    @Test
    void copyConstructorPreservesValues() {
        final SegmentConf original = new SegmentConf(5, 6, 10, 2, 1, 1024,
                0.01D, 1024, List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()));

        final SegmentConf copy = new SegmentConf(original);

        assertEquals(original.getMaxNumberOfKeysInSegmentWriteCache(),
                copy.getMaxNumberOfKeysInSegmentWriteCache());
        assertEquals(original.getMaxNumberOfKeysInSegmentWriteCacheDuringFlush(),
                copy.getMaxNumberOfKeysInSegmentWriteCacheDuringFlush());
        assertEquals(original.getMaxNumberOfKeysInSegmentCache(),
                copy.getMaxNumberOfKeysInSegmentCache());
        assertEquals(original.getMaxNumberOfKeysInChunk(),
                copy.getMaxNumberOfKeysInChunk());
        assertEquals(original.getBloomFilterNumberOfHashFunctions(),
                copy.getBloomFilterNumberOfHashFunctions());
        assertEquals(original.getBloomFilterIndexSizeInBytes(),
                copy.getBloomFilterIndexSizeInBytes());
        assertEquals(original.getBloomFilterProbabilityOfFalsePositive(),
                copy.getBloomFilterProbabilityOfFalsePositive());
        assertEquals(original.getDiskIoBufferSize(),
                copy.getDiskIoBufferSize());
    }

    @Test
    void filterListsAreImmutable() {
        final SegmentConf conf = new SegmentConf(5, 6, 10, 2, 1, 1024, 0.01D,
                1024, List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()));

        assertThrows(UnsupportedOperationException.class,
                () -> conf.getEncodingChunkFilters()
                        .add(new ChunkFilterDoNothing()));
        assertThrows(UnsupportedOperationException.class,
                () -> conf.getDecodingChunkFilters()
                        .add(new ChunkFilterDoNothing()));
    }
}
