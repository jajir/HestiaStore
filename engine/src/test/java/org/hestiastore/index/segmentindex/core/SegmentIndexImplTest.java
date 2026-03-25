package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
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
    void putAsync_usesDedicatedIndexWorkerExecutor() throws Exception {
        final RecordingTypeDescriptorShortString recordingValueTypeDescriptor = new RecordingTypeDescriptorShortString();
        final IndexConfiguration<Integer, String> conf = buildConf(
                recordingValueTypeDescriptor);
        try (IndexInternalConcurrent<Integer, String> asyncIndex = new IndexInternalConcurrent<>(
                new MemDirectory(), new TypeDescriptorInteger(),
                recordingValueTypeDescriptor, conf,
                conf.resolveRuntimeConfiguration(),
                new IndexExecutorRegistry(conf))) {
            asyncIndex.putAsync(1, "one").toCompletableFuture().get(5,
                    TimeUnit.SECONDS);
        }

        final String observedThreadName = recordingValueTypeDescriptor
                .observedThreadName();
        assertTrue(observedThreadName != null
                && observedThreadName.startsWith("index-worker-"));
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return buildConf(new TypeDescriptorShortString());
    }

    private IndexConfiguration<Integer, String> buildConf(
            final TypeDescriptor<String> valueTypeDescriptor) {
        return IndexConfiguration.<Integer, String>builder()
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(valueTypeDescriptor)
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
                .withIndexWorkerThreadCount(1)
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))
                .build();
    }

    private static final class RecordingTypeDescriptorShortString
            extends TypeDescriptorShortString {

        private final AtomicReference<String> observedThreadName = new AtomicReference<>();

        @Override
        public boolean isTombstone(final String value) {
            observedThreadName.compareAndSet(null,
                    Thread.currentThread().getName());
            return super.isTombstone(value);
        }

        String observedThreadName() {
            return observedThreadName.get();
        }
    }
}
