package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CompletionException;
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
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.Wal;
import org.hestiastore.index.segmentindex.WalDurabilityMode;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
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
        index.flushAndWait();

        final KeyToSegmentMapSynchronizedAdapter<Integer> cache = readKeyToSegmentMap(
                index);
        final SegmentRegistryImpl<Integer, String> registry = readSegmentRegistry(
                index);
        final SegmentId segmentId = cache.findSegmentId(1);
        final Segment<Integer, String> segment = registry.getSegment(segmentId)
                .getValue();
        awaitSegmentReady(segment);
        awaitSegmentCount(cache, 2);
        assertEquals(SegmentId.of(1), cache.findSegmentId(1));
    }

    @Test
    void deleteWritesTombstoneToSegment() {
        index.put(1, "one");
        index.delete(1);

        assertNull(index.get(1));
    }

    @Test
    void walSyncFailureTransitionsIndexToErrorState() {
        resetIndex(10, 1, Wal.builder()
                .withDurabilityMode(WalDurabilityMode.SYNC).build());
        injectWalSyncFailure(index, new IllegalStateException("simulated"));

        try {
            final RuntimeException exception = assertThrows(RuntimeException.class,
                    () -> index.put(1, "one"));
            assertTrue(exception.getMessage().contains("WAL sync failure"));
            assertEquals(SegmentIndexState.ERROR, index.getState());
            assertThrows(IllegalStateException.class, () -> index.get(1));
        } finally {
            clearWalSyncFailure(index);
        }
    }

    @Test
    void walSyncFailureDuringCheckpointTransitionsIndexToErrorState() {
        resetIndex(10, 1, Wal.builder()
                .withDurabilityMode(WalDurabilityMode.SYNC).build());
        index.put(1, "one");
        injectWalSyncFailure(index, new IllegalStateException("simulated"));

        try {
            final RuntimeException exception = assertThrows(RuntimeException.class,
                    () -> index.flushAndWait());
            assertTrue(exception.getMessage().contains("WAL sync failure"));
            assertEquals(SegmentIndexState.ERROR, index.getState());
            assertThrows(IllegalStateException.class, () -> index.get(1));
        } finally {
            clearWalSyncFailure(index);
        }
    }

    @Test
    void walSyncFailureOnDeleteTransitionsIndexToErrorState() {
        resetIndex(10, 1, Wal.builder()
                .withDurabilityMode(WalDurabilityMode.SYNC).build());
        index.put(1, "one");
        injectWalSyncFailure(index, new IllegalStateException("simulated"));

        try {
            final RuntimeException exception = assertThrows(RuntimeException.class,
                    () -> index.delete(1));
            assertTrue(exception.getMessage().contains("WAL sync failure"));
            assertEquals(SegmentIndexState.ERROR, index.getState());
            assertThrows(IllegalStateException.class, () -> index.get(1));
        } finally {
            clearWalSyncFailure(index);
        }
    }

    @Test
    void walSyncFailureOnPutAsyncTransitionsIndexToErrorState() {
        resetIndex(10, 1, Wal.builder()
                .withDurabilityMode(WalDurabilityMode.SYNC).build());
        injectWalSyncFailure(index, new IllegalStateException("simulated"));
        final var putFuture = index.putAsync(1, "one").toCompletableFuture();

        try {
            final CompletionException exception = assertThrows(
                    CompletionException.class, putFuture::join);
            assertTrue(exception.getCause().getMessage()
                    .contains("WAL sync failure"));
            assertEquals(SegmentIndexState.ERROR, index.getState());
            assertThrows(IllegalStateException.class, () -> index.get(1));
        } finally {
            clearWalSyncFailure(index);
        }
    }

    @Test
    void walSyncFailureOnDeleteAsyncTransitionsIndexToErrorState() {
        resetIndex(10, 1, Wal.builder()
                .withDurabilityMode(WalDurabilityMode.SYNC).build());
        index.put(1, "one");
        injectWalSyncFailure(index, new IllegalStateException("simulated"));
        final var deleteFuture = index.deleteAsync(1).toCompletableFuture();

        try {
            final CompletionException exception = assertThrows(
                    CompletionException.class, deleteFuture::join);
            assertTrue(exception.getCause().getMessage()
                    .contains("WAL sync failure"));
            assertEquals(SegmentIndexState.ERROR, index.getState());
            assertThrows(IllegalStateException.class, () -> index.get(1));
        } finally {
            clearWalSyncFailure(index);
        }
    }

    @Test
    void walRetentionBackpressureSkipsUnsatisfiableSingleActiveSegmentCase() {
        final Wal wal = Wal.builder()
                .withDurabilityMode(WalDurabilityMode.ASYNC)
                .withSegmentSizeBytes(64L * 1024L)
                .withMaxBytesBeforeForcedCheckpoint(1L).build();
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(tdi)//
                .withValueTypeDescriptor(tds)//
                .withName("wal-retention-single-segment-test")//
                .withMaxNumberOfKeysInSegmentCache(4)//
                .withMaxNumberOfKeysInActivePartition(1)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withMaxNumberOfKeysInSegment(10)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withContextLoggingEnabled(false)//
                .withIndexBusyBackoffMillis(1)//
                .withIndexBusyTimeoutMillis(200)//
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withWal(wal)//
                .build();
        if (index != null && !index.wasClosed()) {
            index.close();
        }
        directory = new MemDirectory();
        index = new TestIndex<>(directory, tdi, tds, conf);

        index.put(1, "one");
        index.put(2, "two");
        assertEquals("one", index.get(1));
        assertEquals("two", index.get(2));
        assertEquals(SegmentIndexState.READY, index.getState());
    }

    private void resetIndex(final int maxKeysInSegment,
            final int maxNumberOfKeysInActivePartition) {
        resetIndex(maxKeysInSegment, maxNumberOfKeysInActivePartition,
                Wal.EMPTY);
    }

    private void resetIndex(final int maxKeysInSegment,
            final int maxNumberOfKeysInActivePartition, final Wal wal) {
        if (index != null && !index.wasClosed()) {
            index.close();
        }
        directory = new MemDirectory();
        index = new TestIndex<>(directory, tdi, tds, buildConf(maxKeysInSegment,
                maxNumberOfKeysInActivePartition, wal));
    }

    private static final class TestIndex<K, V>
            extends IndexInternalConcurrent<K, V> {

        private TestIndex(final Directory directoryFacade,
                final TypeDescriptor<K> keyTypeDescriptor,
                final TypeDescriptor<V> valueTypeDescriptor,
                final IndexConfiguration<K, V> conf) {
            super(directoryFacade, keyTypeDescriptor, valueTypeDescriptor,
                    conf, conf.resolveRuntimeConfiguration(),
                    new IndexExecutorRegistry(conf));
        }

        void awaitSplitsIdlePublic() {
            awaitSplitsIdle();
        }
    }

    private IndexConfiguration<Integer, String> buildConf(
            final int maxKeysInSegment,
            final int maxNumberOfKeysInActivePartition, final Wal wal) {
        return IndexConfiguration.<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(tdi)//
                .withValueTypeDescriptor(tds)//
                .withName("test-index")//
                .withMaxNumberOfKeysInSegmentCache(4)//
                .withMaxNumberOfKeysInActivePartition(
                        maxNumberOfKeysInActivePartition)//
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
                .withWal(wal)//
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
        return index.keyToSegmentMap();
    }

    @SuppressWarnings("unchecked")
    private static <K, V> SegmentRegistryImpl<K, V> readSegmentRegistry(
            final SegmentIndexImpl<K, V> index) {
        return (SegmentRegistryImpl<K, V>) index.segmentRegistry();
    }

    private static void injectWalSyncFailure(final SegmentIndexImpl<?, ?> index,
            final RuntimeException failure) {
        setWalSyncFailure(index, failure);
    }

    private static void clearWalSyncFailure(final SegmentIndexImpl<?, ?> index) {
        setWalSyncFailure(index, null);
    }

    private static void setWalSyncFailure(final SegmentIndexImpl<?, ?> index,
            final RuntimeException failure) {
        try {
            final Object walRuntime = index.walRuntime();
            final Field syncFailureField = walRuntime.getClass()
                    .getDeclaredField("syncFailure");
            syncFailureField.setAccessible(true);
            syncFailureField.set(walRuntime, failure);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException(
                    "Unable to inject WAL sync failure for test", ex);
        }
    }
}
