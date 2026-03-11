package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.junit.jupiter.api.Test;

class SegmentIndexLifecycleTest {

    @Test
    void createOpenInitializesResourcesAndCloseReleasesThem() {
        final MemDirectory directory = new MemDirectory();
        final SegmentIndexLifecycle<Integer, String> lifecycle = new SegmentIndexLifecycle<>(
                directory, buildConf("lifecycle-create", 1));

        lifecycle.open(true);
        assertNotNull(lifecycle.getIndexConfiguration());
        assertNotNull(lifecycle.getManagedExecutorRegistry());
        assertNotNull(lifecycle.getManagedDirectory());

        lifecycle.close();
        assertNull(lifecycle.getIndexConfiguration());
        assertNull(lifecycle.getManagedExecutorRegistry());
        assertNull(lifecycle.getManagedDirectory());
    }

    @Test
    void openMergesStoredConfigurationWithOverrides() {
        final MemDirectory directory = new MemDirectory();
        final SegmentIndexLifecycle<Integer, String> createLifecycle = new SegmentIndexLifecycle<>(
                directory, buildConf("lifecycle-open", 1));
        createLifecycle.open(true);
        createLifecycle.close();

        final SegmentIndexLifecycle<Integer, String> openLifecycle = new SegmentIndexLifecycle<>(
                directory, buildConf("lifecycle-open", 2));
        openLifecycle.open(false);
        assertEquals(2,
                openLifecycle.getIndexConfiguration()
                        .getIndexWorkerThreadCount());
        openLifecycle.close();
    }

    @Test
    void inMemoryConstructorSupportsCreateOpen() {
        final SegmentIndexLifecycle<Integer, String> lifecycle = new SegmentIndexLifecycle<>(
                buildConf("lifecycle-in-memory", 1));
        lifecycle.open(true);
        assertNotNull(lifecycle.getManagedDirectory());
        lifecycle.close();
    }

    private static IndexConfiguration<Integer, String> buildConf(
            final String indexName, final int indexWorkerThreads) {
        return IndexConfiguration.<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorShortString())//
                .withName(indexName)//
                .withContextLoggingEnabled(false)//
                .withMaxNumberOfKeysInSegmentCache(10)//
                .withMaxNumberOfKeysInActivePartition(5)//
                .withMaxNumberOfKeysInPartitionBuffer(6)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withMaxNumberOfKeysInSegment(100)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withIndexWorkerThreadCount(indexWorkerThreads)//
                .withNumberOfSegmentIndexMaintenanceThreads(1)//
                .withNumberOfRegistryLifecycleThreads(1)//
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))//
                .build();
    }
}
