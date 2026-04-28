package org.hestiastore.index.segmentindex.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexConfigurationContract;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.junit.jupiter.api.Test;

class SegmentIndexConfigurationDefaultsUsageTest {

    @Test
    void test_indexCreate_appliesContractDefaults() {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, String> sparseConfiguration = IndexConfiguration
                .<Integer, String>builder()//
                .identity(identity -> identity.keyClass(Integer.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity
                        .keyTypeDescriptor(new TypeDescriptorInteger()))//
                .identity(identity -> identity
                        .valueTypeDescriptor(new TypeDescriptorShortString()))//
                .identity(identity -> identity.name("defaults-check-index"))//
                .build();

        final IndexConfigurationContract defaults = IndexConfigurationRegistry
                .get(Integer.class).orElseThrow(() -> new IllegalStateException(
                        "Missing contract defaults for Integer"));

        try (SegmentIndex<Integer, String> index = SegmentIndex.create(directory, sparseConfiguration)) {
            final IndexConfiguration<Integer, String> actual = index
                    .getConfiguration();

            assertEquals(defaults.segment().cacheKeyLimit(),
                    actual.segment().cacheKeyLimit(),
                    "Segment cache size must come from contract defaults");
            assertEquals(defaults.segment().chunkKeyLimit(),
                    actual.segment().chunkKeyLimit(),
                    "Segment chunk size must come from contract defaults");
            assertEquals(defaults.segment().deltaCacheFileLimit(),
                    actual.segment().deltaCacheFileLimit(),
                    "Delta cache file cap must come from contract defaults");
            assertEquals(defaults.segment().maxKeys(),
                    actual.segment().maxKeys(),
                    "Segment key count must come from contract defaults");
            assertEquals(defaults.segment().cachedSegmentLimit(),
                    actual.segment().cachedSegmentLimit(),
                    "Segments in cache must come from contract defaults");
            assertEquals(defaults.maintenance().registryLifecycleThreads(),
                    actual.maintenance().registryLifecycleThreads(),
                    "Registry lifecycle threads must come from contract defaults");
            assertEquals(defaults.io().diskBufferSizeBytes(),
                    actual.io().diskBufferSizeBytes(),
                    "Disk IO buffer size must come from contract defaults");
            assertEquals(defaults.bloomFilter().hashFunctions(),
                    actual.bloomFilter().hashFunctions(),
                    "Bloom hash count must come from contract defaults");
            assertEquals(defaults.bloomFilter().indexSizeBytes(),
                    actual.bloomFilter().indexSizeBytes(),
                    "Bloom index size must come from contract defaults");
            assertEquals(
                    defaults.bloomFilter().falsePositiveProbability(),
                    actual.bloomFilter().falsePositiveProbability(),
                    "Bloom false-positive probability must come from contract defaults");
            assertEquals(defaults.logging().contextEnabled(),
                    actual.logging().contextEnabled(),
                    "Context logging flag must come from contract defaults");

            assertIterableEquals(
                    defaults.filters().encodingChunkFilterSpecs(),
                    actual.resolveRuntimeConfiguration().getEncodingChunkFilterSpecs(),
                    "Encoding filters must come from contract defaults");
            assertIterableEquals(
                    defaults.filters().decodingChunkFilterSpecs(),
                    actual.resolveRuntimeConfiguration().getDecodingChunkFilterSpecs(),
                    "Decoding filters must come from contract defaults");
        }
    }
}
