package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.junit.jupiter.api.Test;

class SegmentConfTest {

    @Test
    void copyConstructorPreservesValues() {
        final SegmentConf original = SegmentConf.builder()
                .withMaxNumberOfKeysInSegmentWriteCache(5)
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(6)
                .withMaxNumberOfKeysInSegmentCache(10)
                .withMaxNumberOfKeysInChunk(2)
                .withMaxNumberOfDeltaCacheFiles(4)
                .withBloomFilterNumberOfHashFunctions(1)
                .withBloomFilterIndexSizeInBytes(1024)
                .withBloomFilterProbabilityOfFalsePositive(0.01D)
                .withDiskIoBufferSize(1024)
                .withEncodingChunkFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingChunkFilters(List.of(new ChunkFilterDoNothing()))
                .build();

        final SegmentConf copy = new SegmentConf(original);

        assertEquals(original.getMaxNumberOfKeysInSegmentWriteCache(),
                copy.getMaxNumberOfKeysInSegmentWriteCache());
        assertEquals(original.getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(),
                copy.getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance());
        assertEquals(original.getMaxNumberOfKeysInSegmentCache(),
                copy.getMaxNumberOfKeysInSegmentCache());
        assertEquals(original.getMaxNumberOfKeysInChunk(),
                copy.getMaxNumberOfKeysInChunk());
        assertEquals(original.getMaxNumberOfDeltaCacheFiles(),
                copy.getMaxNumberOfDeltaCacheFiles());
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
        final ChunkFilterDoNothing encodingFilterToAdd = new ChunkFilterDoNothing();
        final ChunkFilterDoNothing decodingFilterToAdd = new ChunkFilterDoNothing();
        final SegmentConf conf = SegmentConf.builder()
                .withMaxNumberOfKeysInSegmentWriteCache(5)
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(6)
                .withMaxNumberOfKeysInSegmentCache(10)
                .withMaxNumberOfKeysInChunk(2)
                .withMaxNumberOfDeltaCacheFiles(4)
                .withBloomFilterNumberOfHashFunctions(1)
                .withBloomFilterIndexSizeInBytes(1024)
                .withBloomFilterProbabilityOfFalsePositive(0.01D)
                .withDiskIoBufferSize(1024)
                .withEncodingChunkFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingChunkFilters(List.of(new ChunkFilterDoNothing()))
                .build();
        final List<ChunkFilter> encodingFilters = conf
                .getEncodingChunkFilters();
        final List<ChunkFilter> decodingFilters = conf
                .getDecodingChunkFilters();

        assertThrows(UnsupportedOperationException.class,
                () -> encodingFilters.add(encodingFilterToAdd));
        assertThrows(UnsupportedOperationException.class,
                () -> decodingFilters.add(decodingFilterToAdd));
    }
}
