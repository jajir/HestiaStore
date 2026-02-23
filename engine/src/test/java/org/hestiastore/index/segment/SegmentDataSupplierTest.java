package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.bloomfilter.BloomFilter;
import org.hestiastore.index.bloomfilter.BloomFilterNull;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.scarceindex.ScarceSegmentIndex;
import org.junit.jupiter.api.Test;

class SegmentDataSupplierTest {

    @Test
    void supplierCreatesNewInstancesPerCall() {
        final SegmentFiles<Integer, String> files = new SegmentFiles<>(
                new MemDirectory(), SegmentId.of(1),
                new TypeDescriptorInteger(), new TypeDescriptorShortString(),
                1024, List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()), 1L);
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
        final SegmentDataSupplier<Integer, String> supplier = new SegmentDataSupplier<>(
                files, conf);

        final BloomFilter<Integer> bloom1 = supplier.getBloomFilter();
        final BloomFilter<Integer> bloom2 = supplier.getBloomFilter();
        assertNotNull(bloom1);
        assertNotNull(bloom2);
        assertNotSame(bloom1, bloom2);
        bloom1.close();
        bloom2.close();

        final ScarceSegmentIndex<Integer> scarce1 = supplier.getScarceIndex();
        final ScarceSegmentIndex<Integer> scarce2 = supplier.getScarceIndex();
        assertNotNull(scarce1);
        assertNotNull(scarce2);
        assertNotSame(scarce1, scarce2);
        scarce1.close();
        scarce2.close();
    }

    @Test
    void supplierHandlesUnsetBloomFilterConfig() {
        final SegmentFiles<Integer, String> files = new SegmentFiles<>(
                new MemDirectory(), SegmentId.of(2),
                new TypeDescriptorInteger(), new TypeDescriptorShortString(),
                1024, List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()), 1L);
        final SegmentConf conf = SegmentConf.builder()
                .withMaxNumberOfKeysInSegmentWriteCache(5)
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(6)
                .withMaxNumberOfKeysInSegmentCache(10)
                .withMaxNumberOfKeysInChunk(2)
                .withMaxNumberOfDeltaCacheFiles(4)
                .withBloomFilterNumberOfHashFunctions(
                        SegmentConf.UNSET_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS)
                .withBloomFilterIndexSizeInBytes(
                        SegmentConf.UNSET_BLOOM_FILTER_INDEX_SIZE_IN_BYTES)
                .withBloomFilterProbabilityOfFalsePositive(
                        SegmentConf.UNSET_BLOOM_FILTER_PROBABILITY)
                .withDiskIoBufferSize(1024)
                .withEncodingChunkFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingChunkFilters(List.of(new ChunkFilterDoNothing()))
                .build();
        final SegmentDataSupplier<Integer, String> supplier = new SegmentDataSupplier<>(
                files, conf);

        try (BloomFilter<Integer> bloom = supplier.getBloomFilter()) {
            assertTrue(bloom instanceof BloomFilterNull,
                    "Expected null-object BloomFilter when sizing is absent");
        }
    }
}
