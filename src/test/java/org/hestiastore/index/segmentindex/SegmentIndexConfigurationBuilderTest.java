package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkData;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Writing;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberWriting;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.junit.jupiter.api.Test;

class SegmentIndexConfigurationBuilderTest {

    @Test
    void withKeyTypeDescriptorFromDescriptor() {
        final TypeDescriptorInteger descriptor = new TypeDescriptorInteger();
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withKeyTypeDescriptor(descriptor).build();
        assertEquals(descriptor.getClass().getName(),
                config.getKeyTypeDescriptor());
    }

    @Test
    void withValueTypeDescriptorFromDescriptor() {
        final TypeDescriptorLong descriptor = new TypeDescriptorLong();
        final IndexConfiguration<Integer, Long> config = IndexConfiguration
                .<Integer, Long>builder()
                .withValueTypeDescriptor(descriptor).build();
        assertEquals(descriptor.getClass().getName(),
                config.getValueTypeDescriptor());
    }

    @Test
    void withKeyTypeDescriptorString() {
        final String descriptor = "key-descriptor";
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withKeyTypeDescriptor(descriptor).build();
        assertEquals(descriptor, config.getKeyTypeDescriptor());
    }

    @Test
    void withValueTypeDescriptorString() {
        final String descriptor = "value-descriptor";
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withValueTypeDescriptor(descriptor).build();
        assertEquals(descriptor, config.getValueTypeDescriptor());
    }

    @Test
    void withKeyClassSetsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withKeyClass(Integer.class).build();
        assertEquals(Integer.class, config.getKeyClass());
    }

    @Test
    void withValueClassSetsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withValueClass(String.class).build();
        assertEquals(String.class, config.getValueClass());
    }

    @Test
    void withNameSetsValue() {
        final String name = "test-index";
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withName(name).build();
        assertEquals(name, config.getIndexName());
    }

    @Test
    void withMaxNumberOfKeysInSegmentCacheSetsValue() {
        final int value = 123;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withMaxNumberOfKeysInSegmentCache(value)
                .build();
        assertEquals(value, config.getMaxNumberOfKeysInSegmentCache());
    }

    @Test
    void withMaxNumberOfKeysInSegmentChunkSetsValue() {
        final int value = 321;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withMaxNumberOfKeysInSegmentChunk(value).build();
        assertEquals(value, config.getMaxNumberOfKeysInSegmentChunk());
    }

    @Test
    void withMaxNumberOfDeltaCacheFilesSetsValue() {
        final int value = 7;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withMaxNumberOfDeltaCacheFiles(value).build();
        assertEquals(value, config.getMaxNumberOfDeltaCacheFiles());
    }

    @Test
    void withMaxNumberOfKeysInCacheSetsValue() {
        final int value = 555;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withMaxNumberOfKeysInCache(value).build();
        assertEquals(value, config.getMaxNumberOfKeysInCache());
    }

    @Test
    void withMaxNumberOfKeysInSegmentSetsValue() {
        final int value = 777;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withMaxNumberOfKeysInSegment(value).build();
        assertEquals(value, config.getMaxNumberOfKeysInSegment());
    }

    @Test
    void withMaxNumberOfSegmentsInCacheSetsValue() {
        final int value = 42;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withMaxNumberOfSegmentsInCache(value).build();
        assertEquals(value, config.getMaxNumberOfSegmentsInCache());
    }

    @Test
    void withBloomFilterNumberOfHashFunctionsSetsValue() {
        final int value = 12;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withBloomFilterNumberOfHashFunctions(value).build();
        assertEquals(value, config.getBloomFilterNumberOfHashFunctions());
    }

    @Test
    void withBloomFilterProbabilityOfFalsePositiveSetsValue() {
        final double value = 0.123d;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withBloomFilterProbabilityOfFalsePositive(value).build();
        assertEquals(value,
                config.getBloomFilterProbabilityOfFalsePositive());
    }

    @Test
    void withIndexWorkerThreadCountSetsValue() {
        final int value = 4;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withIndexWorkerThreadCount(value).build();
        assertEquals(value, config.getIndexWorkerThreadCount());
    }

    @Test
    void withNumberOfIoThreadsSetsValue() {
        final int value = 3;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withNumberOfIoThreads(value).build();
        assertEquals(value, config.getNumberOfIoThreads());
    }

    @Test
    void withNumberOfRegistryLifecycleThreadsSetsValue() {
        final int value = 5;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withNumberOfRegistryLifecycleThreads(value).build();
        assertEquals(value, config.getNumberOfRegistryLifecycleThreads());
    }

    @Test
    void withBloomFilterIndexSizeInBytesSetsValue() {
        final int value = 64;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withBloomFilterIndexSizeInBytes(value).build();
        assertEquals(value, config.getBloomFilterIndexSizeInBytes());
    }

    @Test
    void withDiskIoBufferSizeInBytesSetsValue() {
        final int value = 4096;
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withDiskIoBufferSizeInBytes(value).build();
        assertEquals(value, config.getDiskIoBufferSize());
    }

    @Test
    void withContextLoggingEnabledSetsValue() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withContextLoggingEnabled(Boolean.FALSE).build();
        assertFalse(config.isContextLoggingEnabled());
    }

    @Test
    void addEncodingFilterAddsInstance() {
        final ChunkFilter filter = new NoOpFilter();
        final IndexConfiguration<Integer, String> config = newBuilder()
                .addEncodingFilter(filter).build();
        assertEquals(1, config.getEncodingChunkFilters().size());
        assertSame(filter, config.getEncodingChunkFilters().get(0));
    }

    @Test
    void addEncodingFilterByClassInstantiatesFilter() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .addEncodingFilter(ChunkFilterCrc32Writing.class).build();
        assertEquals(1, config.getEncodingChunkFilters().size());
        assertEquals(ChunkFilterCrc32Writing.class,
                config.getEncodingChunkFilters().get(0).getClass());
    }

    @Test
    void withEncodingFilterClassesReplacesList() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .addEncodingFilter(new NoOpFilter())
                .withEncodingFilterClasses(List.of(ChunkFilterCrc32Writing.class,
                        ChunkFilterMagicNumberWriting.class))
                .build();
        assertEquals(2, config.getEncodingChunkFilters().size());
        assertEquals(ChunkFilterCrc32Writing.class,
                config.getEncodingChunkFilters().get(0).getClass());
        assertEquals(ChunkFilterMagicNumberWriting.class,
                config.getEncodingChunkFilters().get(1).getClass());
    }

    @Test
    void withEncodingFiltersReplacesWithProvidedInstances() {
        final NoOpFilter first = new NoOpFilter();
        final NoOpFilter second = new NoOpFilter();
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withEncodingFilters(List.of(first, second)).build();
        assertEquals(List.of(first, second),
                config.getEncodingChunkFilters());
    }

    @Test
    void addDecodingFilterAddsInstance() {
        final NoOpFilter filter = new NoOpFilter();
        final IndexConfiguration<Integer, String> config = newBuilder()
                .addDecodingFilter(filter).build();
        assertEquals(1, config.getDecodingChunkFilters().size());
        assertSame(filter, config.getDecodingChunkFilters().get(0));
    }

    @Test
    void addDecodingFilterByClassInstantiatesFilter() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .addDecodingFilter(ChunkFilterCrc32Writing.class).build();
        assertEquals(1, config.getDecodingChunkFilters().size());
        assertEquals(ChunkFilterCrc32Writing.class,
                config.getDecodingChunkFilters().get(0).getClass());
    }

    @Test
    void withDecodingFilterClassesReplacesList() {
        final IndexConfiguration<Integer, String> config = newBuilder()
                .addDecodingFilter(new NoOpFilter())
                .withDecodingFilterClasses(List.of(ChunkFilterCrc32Writing.class,
                        ChunkFilterMagicNumberWriting.class))
                .build();
        assertEquals(2, config.getDecodingChunkFilters().size());
        assertEquals(ChunkFilterCrc32Writing.class,
                config.getDecodingChunkFilters().get(0).getClass());
        assertEquals(ChunkFilterMagicNumberWriting.class,
                config.getDecodingChunkFilters().get(1).getClass());
    }

    @Test
    void withDecodingFiltersReplacesWithProvidedInstances() {
        final NoOpFilter first = new NoOpFilter();
        final NoOpFilter second = new NoOpFilter();
        final IndexConfiguration<Integer, String> config = newBuilder()
                .withDecodingFilters(List.of(first, second)).build();
        assertEquals(List.of(first, second),
                config.getDecodingChunkFilters());
    }

    private IndexConfigurationBuilder<Integer, String> newBuilder() {
        return IndexConfiguration.<Integer, String>builder();
    }

    private static class NoOpFilter implements ChunkFilter {
        @Override
        public ChunkData apply(final ChunkData input) {
            return input;
        }
    }
}
