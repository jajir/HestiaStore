package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.lang.reflect.Field;
import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstore.ChunkFilterProviderRegistry;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.core.session.IndexInternalConcurrent;
import org.junit.jupiter.api.Test;

class SegmentIndexManagedIndexFactoryTest {

    @Test
    void createWrapsRuntimeIndexWithContextLoggingWhenEnabled() {
        final SegmentIndex<Integer, String> index = createIndex(
                buildConf("managed-index-factory-logging", true));

        try {
            assertInstanceOf(IndexDirectoryClosingAdapter.class, index);
            assertInstanceOf(IndexContextLoggingAdapter.class, wrappedIndex(index));
        } finally {
            index.close();
        }
    }

    @Test
    void createWrapsConcurrentRuntimeIndexWhenContextLoggingDisabled() {
        final SegmentIndex<Integer, String> index = createIndex(
                buildConf("managed-index-factory-plain", false));

        try {
            assertInstanceOf(IndexDirectoryClosingAdapter.class, index);
            assertInstanceOf(IndexInternalConcurrent.class, wrappedIndex(index));
        } finally {
            index.close();
        }
    }

    private static SegmentIndex<Integer, String> createIndex(
            final IndexConfiguration<Integer, String> configuration) {
        return SegmentIndexManagedIndexFactory.create(
                SegmentIndexLifecycleOpenFlow.startCreatedLifecycle(
                        new MemDirectory(), configuration,
                        ChunkFilterProviderRegistry.defaultRegistry()));
    }

    private static Object wrappedIndex(final SegmentIndex<Integer, String> index) {
        try {
            final Field field = index.getClass().getDeclaredField("delegate");
            field.setAccessible(true);
            return field.get(index);
        } catch (final ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static IndexConfiguration<Integer, String> buildConf(
            final String indexName, final boolean contextLoggingEnabled) {
        return IndexConfiguration.<Integer, String>builder()
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorShortString())
                .withName(indexName)
                .withContextLoggingEnabled(contextLoggingEnabled)
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
}
