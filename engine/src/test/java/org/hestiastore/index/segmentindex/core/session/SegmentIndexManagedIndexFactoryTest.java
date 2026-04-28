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
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity.valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name(indexName))
                .logging(logging -> logging.contextEnabled(contextLoggingEnabled))
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
}
