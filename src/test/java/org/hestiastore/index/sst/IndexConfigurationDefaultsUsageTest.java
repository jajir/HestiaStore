package org.hestiastore.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import java.util.List;
import java.util.stream.Collectors;

import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class IndexConfigurationDefaultsUsageTest {

    @Test
    void test_indexCreate_appliesContractDefaults() {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, String> sparseConfiguration = IndexConfiguration
                .<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withName("defaults-check-index")//
                .build();

        final IndexConfigurationContract defaults = IndexConfigurationRegistry
                .get(Integer.class)
                .orElseThrow(
                        () -> new IllegalStateException("Missing contract defaults for Integer"));

        try (Index<Integer, String> index = Index.create(directory,
                sparseConfiguration)) {
            final IndexConfiguration<Integer, String> actual = index
                    .getConfiguration();

            assertEquals(defaults.getMaxNumberOfKeysInSegmentCache(),
                    actual.getMaxNumberOfKeysInSegmentCache(),
                    "Segment cache size must come from contract defaults");
            assertEquals(
                    defaults.getMaxNumberOfKeysInSegmentCacheDuringFlushing(),
                    actual.getMaxNumberOfKeysInSegmentCacheDuringFlushing(),
                    "Flushing cache size must come from contract defaults");
            assertEquals(defaults.getMaxNumberOfKeysInSegmentChunk(),
                    actual.getMaxNumberOfKeysInSegmentChunk(),
                    "Segment chunk size must come from contract defaults");
            assertEquals(defaults.getMaxNumberOfKeysInCache(),
                    actual.getMaxNumberOfKeysInCache(),
                    "Index cache size must come from contract defaults");
            assertEquals(defaults.getMaxNumberOfKeysInSegment(),
                    actual.getMaxNumberOfKeysInSegment(),
                    "Segment key count must come from contract defaults");
            assertEquals(defaults.getMaxNumberOfSegmentsInCache(),
                    actual.getMaxNumberOfSegmentsInCache(),
                    "Segments in cache must come from contract defaults");
            assertEquals(defaults.getDiskIoBufferSizeInBytes(),
                    actual.getDiskIoBufferSize(),
                    "Disk IO buffer size must come from contract defaults");
            assertEquals(defaults.getBloomFilterNumberOfHashFunctions(),
                    actual.getBloomFilterNumberOfHashFunctions(),
                    "Bloom hash count must come from contract defaults");
            assertEquals(defaults.getBloomFilterIndexSizeInBytes(),
                    actual.getBloomFilterIndexSizeInBytes(),
                    "Bloom index size must come from contract defaults");
            assertEquals(
                    Double.valueOf(
                            defaults.getBloomFilterProbabilityOfFalsePositive()),
                    actual.getBloomFilterProbabilityOfFalsePositive(),
                    "Bloom false-positive probability must come from contract defaults");
            assertEquals(Boolean.valueOf(defaults.isThreadSafe()),
                    actual.isThreadSafe(),
                    "Thread-safety flag must come from contract defaults");
            assertEquals(Boolean.valueOf(defaults.isContextLoggingEnabled()),
                    actual.isContextLoggingEnabled(),
                    "Context logging flag must come from contract defaults");

            assertIterableEquals(
                    toFilterClasses(defaults.getEncodingChunkFilters()),
                    toFilterClasses(actual.getEncodingChunkFilters()),
                    "Encoding filters must come from contract defaults");
            assertIterableEquals(
                    toFilterClasses(defaults.getDecodingChunkFilters()),
                    toFilterClasses(actual.getDecodingChunkFilters()),
                    "Decoding filters must come from contract defaults");
        }
    }

    private List<Class<? extends ChunkFilter>> toFilterClasses(
            final List<ChunkFilter> filters) {
        return filters.stream().map(ChunkFilter::getClass)
                .collect(Collectors.toList());
    }
}
