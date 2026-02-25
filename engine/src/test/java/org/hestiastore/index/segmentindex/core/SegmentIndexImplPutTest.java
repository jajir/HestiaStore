package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.split.SegmentSplitCoordinator;
import org.hestiastore.index.segmentindex.split.SegmentWriterTxFactory;
import org.hestiastore.index.segmentregistry.SegmentFactory;
import org.hestiastore.index.segmentregistry.SegmentRegistryImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SegmentIndexImplPutTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();
    private TestIndex<Integer, String> index;
    private Directory directory;

    @BeforeEach
    void setUp() {
        resetIndex(10, 1);
    }

    @AfterEach
    void tearDown() {
        if (index != null && !index.wasClosed()) {
            index.close();
        }
    }

    @Test
    void putWritesDirectlyToSegment() {
        index.put(1, "one");

        assertEquals("one", index.get(1));
    }

    @Test
    void putRejectsTombstoneValues() {
        resetIndex(10, 2);

        assertThrows(IllegalArgumentException.class,
                () -> index.put(1, TypeDescriptorShortString.TOMBSTONE_VALUE));
    }

    @Test
    void putOptionallySplitsSegmentWhenThresholdReached() {
        resetIndex(4, 1);

        index.put(1, "a");
        index.put(2, "b");
        index.put(3, "c");
        index.put(4, "d");
        index.put(5, "e");

        final KeyToSegmentMapSynchronizedAdapter<Integer> cache = readKeyToSegmentMap(
                index);
        index.awaitSplitsIdlePublic();
        final SegmentRegistryImpl<Integer, String> registry = readSegmentRegistry(
                index);
        final SegmentId segmentId = cache.findSegmentId(1);
        final Segment<Integer, String> segment = registry.getSegment(segmentId)
                .getValue();
        awaitSegmentReady(segment);
        final ExecutorService segmentMaintenancePool = Executors
                .newSingleThreadExecutor();
        try {
            final SegmentFactory<Integer, String> segmentFactory = new SegmentFactory<>(
                    directory, tdi, tds, index.getConfiguration(),
                    segmentMaintenancePool);
            final SegmentWriterTxFactory<Integer, String> writerTxFactory = id -> segmentFactory
                    .newSegmentBuilder(id).openWriterTx();
            final SegmentSplitCoordinator<Integer, String> splitCoordinator = new SegmentSplitCoordinator<>(
                    index.getConfiguration(), cache, registry, writerTxFactory);
            splitCoordinator.optionallySplit(segment);
            awaitSegmentCount(cache, 2);
            assertEquals(SegmentId.of(1), cache.findSegmentId(1));
        } finally {
            segmentMaintenancePool.shutdownNow();
        }
    }

    @Test
    void deleteWritesTombstoneToSegment() {
        index.put(1, "one");
        index.delete(1);

        assertNull(index.get(1));
    }

    private void resetIndex(final int maxKeysInSegment,
            final int maxNumberOfKeysInSegmentWriteCache) {
        if (index != null && !index.wasClosed()) {
            index.close();
        }
        directory = new MemDirectory();
        index = new TestIndex<>(directory, tdi, tds, buildConf(maxKeysInSegment,
                maxNumberOfKeysInSegmentWriteCache));
    }

    private static final class TestIndex<K, V>
            extends IndexInternalConcurrent<K, V> {

        private TestIndex(final Directory directoryFacade,
                final TypeDescriptor<K> keyTypeDescriptor,
                final TypeDescriptor<V> valueTypeDescriptor,
                final IndexConfiguration<K, V> conf) {
            super(directoryFacade, keyTypeDescriptor, valueTypeDescriptor,
                    conf, new IndexExecutorRegistry(conf));
        }

        void awaitSplitsIdlePublic() {
            awaitSplitsIdle();
        }
    }

    private IndexConfiguration<Integer, String> buildConf(
            final int maxKeysInSegment,
            final int maxNumberOfKeysInSegmentWriteCache) {
        return IndexConfiguration.<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(tdi)//
                .withValueTypeDescriptor(tds)//
                .withName("test-index")//
                .withMaxNumberOfKeysInSegmentCache(4)//
                .withMaxNumberOfKeysInSegmentWriteCache(
                        maxNumberOfKeysInSegmentWriteCache)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withMaxNumberOfKeysInSegment(maxKeysInSegment)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withContextLoggingEnabled(false)//
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))//
                .build();
    }

    private static void awaitSegmentCount(
            final KeyToSegmentMapSynchronizedAdapter<Integer> cache,
            final int expectedCount) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < deadline) {
            if (cache.getSegmentIds().size() == expectedCount) {
                return;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));
            if (Thread.currentThread().isInterrupted()) {
                break;
            }
        }
        assertEquals(expectedCount, cache.getSegmentIds().size());
    }

    private static void awaitSegmentReady(final Segment<?, ?> segment) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < deadline) {
            final SegmentState state = segment.getState();
            if (state == SegmentState.READY || state == SegmentState.CLOSED) {
                return;
            }
            if (state == SegmentState.ERROR) {
                throw new AssertionError("Segment failed during maintenance.");
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("Timed out waiting for READY segment.");
    }

    @SuppressWarnings("unchecked")
    private static <K, V> KeyToSegmentMapSynchronizedAdapter<K> readKeyToSegmentMap(
            final SegmentIndexImpl<K, V> index) {
        try {
            final Field field = SegmentIndexImpl.class
                    .getDeclaredField("keyToSegmentMap");
            field.setAccessible(true);
            return (KeyToSegmentMapSynchronizedAdapter<K>) field.get(index);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException(
                    "Unable to read keyToSegmentMap for test", ex);
        }
    }

    @SuppressWarnings("unchecked")
    private static <K, V> SegmentRegistryImpl<K, V> readSegmentRegistry(
            final SegmentIndexImpl<K, V> index) {
        try {
            final Field field = SegmentIndexImpl.class
                    .getDeclaredField("segmentRegistry");
            field.setAccessible(true);
            return (SegmentRegistryImpl<K, V>) field.get(index);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException(
                    "Unable to read segmentRegistry for test", ex);
        }
    }
}
