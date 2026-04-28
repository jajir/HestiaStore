package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryFixture;

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
                ExecutorRegistryFixture.from(conf));
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
                new MemDirectory(), conf, ExecutorRegistryFixture.from(conf))) {
            assertEquals(SegmentIndexState.READY, doubleStartupIndex.getState());
            assertEquals(0, doubleStartupIndex.getStartupConsistencyChecks());

            doubleStartupIndex.completeStartupAgainForTest();

            assertEquals(SegmentIndexState.READY, doubleStartupIndex.getState());
            assertEquals(0, doubleStartupIndex.getStartupConsistencyChecks());
        }
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity.valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name("segment-index-impl-test"))
                .logging(logging -> logging.contextEnabled(false))
                .segment(segment -> segment.cacheKeyLimit(10))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(6))
                .segment(segment -> segment.chunkKeyLimit(2))
                .segment(segment -> segment.maxKeys(100))
                .segment(segment -> segment.cachedSegmentLimit(3))
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))
                .bloomFilter(bloomFilter -> bloomFilter.falsePositiveProbability(0.01D))
                .io(io -> io.diskBufferSizeBytes(1024))
                .filters(filters -> filters.encodingFilters(List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters.decodingFilters(List.of(new ChunkFilterDoNothing())))
                .build();
    }

    private static final class DoubleStartupIndex
            extends SegmentIndexImpl<Integer, String> {

        private final AtomicInteger startupConsistencyChecks = new AtomicInteger();

        private DoubleStartupIndex(final Directory directoryFacade,
                final IndexConfiguration<Integer, String> conf,
                final ExecutorRegistry executorRegistry) {
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
