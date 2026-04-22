package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.segmentindex.core.maintenance.IndexExecutorRegistry;
import org.hestiastore.index.segmentindex.core.session.IndexInternalConcurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRuntimeConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SegmentIndexImplTest {

    private IndexInternalConcurrent<Integer, String> index;

    @BeforeEach
    void setUp() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        index = new IndexInternalConcurrent<>(
                new MemDirectory(),
                new TypeDescriptorInteger(),
                new TypeDescriptorShortString(),
                conf, conf.resolveRuntimeConfiguration(),
                new IndexExecutorRegistry(conf));
    }

    @AfterEach
    void tearDown() {
        if (index != null && !index.wasClosed()) {
            index.close();
        }
    }

    @Test
    void putGetAndDeleteRoundTrip() {
        index.put(1, "one");

        assertEquals("one", index.get(1));

        index.delete(1);
        assertNull(index.get(1));
    }

    @Test
    void constructorCompletesStartupOnlyOnce() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        try (DoubleStartupIndex doubleStartupIndex = new DoubleStartupIndex(
                new MemDirectory(), conf, new IndexExecutorRegistry(conf))) {
            assertEquals(SegmentIndexState.READY, doubleStartupIndex.getState());
            assertEquals(0, doubleStartupIndex.getStartupConsistencyChecks());

            doubleStartupIndex.completeStartupAgainForTest();

            assertEquals(SegmentIndexState.READY, doubleStartupIndex.getState());
            assertEquals(0, doubleStartupIndex.getStartupConsistencyChecks());
        }
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorShortString())
                .withName("segment-index-impl-test")
                .withContextLoggingEnabled(false)
                .withMaxNumberOfKeysInSegmentCache(10)
                .withMaxNumberOfKeysInActivePartition(5)
                .withMaxNumberOfKeysInPartitionBuffer(6)
                .withMaxNumberOfKeysInSegmentChunk(2)
                .withMaxNumberOfKeysInSegment(100)
                .withMaxNumberOfSegmentsInCache(3)
                .withBloomFilterNumberOfHashFunctions(1)
                .withBloomFilterIndexSizeInBytes(1024)
                .withBloomFilterProbabilityOfFalsePositive(0.01D)
                .withDiskIoBufferSizeInBytes(1024)
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))
                .build();
    }

    private static final class DoubleStartupIndex
            extends SegmentIndexImpl<Integer, String> {

        private final AtomicInteger startupConsistencyChecks = new AtomicInteger();

        private DoubleStartupIndex(final Directory directoryFacade,
                final IndexConfiguration<Integer, String> conf,
                final IndexExecutorRegistry executorRegistry) {
            super(directoryFacade, new TypeDescriptorInteger(),
                    new TypeDescriptorShortString(), conf,
                    runtimeConfiguration(conf), executorRegistry);
            completeStartup();
        }

        @Override
        protected void onStartupConsistencyCheck() {
            startupConsistencyChecks.incrementAndGet();
        }

        private void completeStartupAgainForTest() {
            completeStartup();
        }

        private int getStartupConsistencyChecks() {
            return startupConsistencyChecks.get();
        }

        private static IndexRuntimeConfiguration<Integer, String> runtimeConfiguration(
                final IndexConfiguration<Integer, String> conf) {
            return conf.resolveRuntimeConfiguration();
        }
    }
}
