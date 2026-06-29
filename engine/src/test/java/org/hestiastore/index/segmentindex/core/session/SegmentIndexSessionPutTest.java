package org.hestiastore.index.segmentindex.core.session;

import static org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationTestSupport.effective;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.api.IndexWalConfiguration;
import org.hestiastore.index.segmentindex.configuration.api.WalDurabilityMode;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SegmentIndexSessionPutTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();
    private SegmentIndex<Integer, String> index;
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
        resetIndex(6, 1);

        index.put(1, "a");
        index.put(2, "b");
        index.put(3, "c");
        index.put(4, "d");
        index.put(5, "e");
        index.put(6, "f");
        index.put(7, "g");
        index.maintenance().flushAndWait();

        final SegmentRouteMap<Integer> cache = readKeyToSegmentMap(
                index);
        final SegmentRegistry<Integer, String> registry = readSegmentRegistry(
                index);
        final SegmentId segmentId = cache.findSegmentIdForKey(1);
        final Segment<Integer, String> segment = registry.loadSegment(segmentId)
                .getSegment();
        awaitSegmentReady(segment);
        awaitSegmentCount(cache, 2);
        assertEquals(SegmentId.of(1), cache.findSegmentIdForKey(1));
        assertEquals("a", index.get(1));
        assertEquals("b", index.get(2));
        assertEquals("c", index.get(3));
        assertEquals("d", index.get(4));
        assertEquals("e", index.get(5));
        assertEquals("f", index.get(6));
        assertEquals("g", index.get(7));

        index.put(8, "h");

        assertEquals("h", index.get(8));
    }

    @Test
    void ascendingWritesRouteThroughPersistedOpenTailAfterReopen() {
        final IndexConfiguration<Integer, String> conf = buildConf(100, 2,
                IndexWalConfiguration.EMPTY);
        if (index != null && !index.wasClosed()) {
            index.close();
        }
        index = null;
        directory = new MemDirectory();

        try (SegmentIndex<Integer, String> created = SegmentIndex.create(
                directory, conf)) {
            created.put(10, "ten");
            created.put(20, "twenty");
            created.maintenance().flushAndWait();
        }

        try (SegmentIndex<Integer, String> reopened = SegmentIndex.open(
                directory, conf)) {
            assertEquals("ten", reopened.get(10));
            assertEquals("twenty", reopened.get(20));
            assertNull(reopened.get(30));
        }
    }

    @Test
    void deleteWritesTombstoneToSegment() {
        index.put(1, "one");
        index.delete(1);

        assertNull(index.get(1));
    }

    @Test
    void walSyncFailureTransitionsIndexToErrorState() {
        resetIndex(10, 1, IndexWalConfiguration.builder()
                .durability(WalDurabilityMode.SYNC).build());
        injectWalSyncFailure(index, new IllegalStateException("simulated"));

        try {
            final RuntimeException exception = assertThrows(RuntimeException.class,
                    () -> index.put(1, "one"));
            assertTrue(exception.getMessage().contains("WAL sync failure"));
            assertEquals(SegmentIndexState.ERROR, index.runtimeMonitoring().snapshot().state());
            assertThrows(IllegalStateException.class, () -> index.get(1));
        } finally {
            clearWalSyncFailure(index);
        }
    }

    @Test
    void walSyncFailureDuringCheckpointTransitionsIndexToErrorState() {
        resetIndex(10, 1, IndexWalConfiguration.builder()
                .durability(WalDurabilityMode.SYNC).build());
        index.put(1, "one");
        injectWalSyncFailure(index, new IllegalStateException("simulated"));

        try {
            final RuntimeException exception = assertThrows(RuntimeException.class,
                    () -> index.maintenance().flushAndWait());
            assertTrue(exception.getMessage().contains("WAL sync failure"));
            assertEquals(SegmentIndexState.ERROR, index.runtimeMonitoring().snapshot().state());
            assertThrows(IllegalStateException.class, () -> index.get(1));
        } finally {
            clearWalSyncFailure(index);
        }
    }

    @Test
    void walSyncFailureOnDeleteTransitionsIndexToErrorState() {
        resetIndex(10, 1, IndexWalConfiguration.builder()
                .durability(WalDurabilityMode.SYNC).build());
        index.put(1, "one");
        injectWalSyncFailure(index, new IllegalStateException("simulated"));

        try {
            final RuntimeException exception = assertThrows(RuntimeException.class,
                    () -> index.delete(1));
            assertTrue(exception.getMessage().contains("WAL sync failure"));
            assertEquals(SegmentIndexState.ERROR, index.runtimeMonitoring().snapshot().state());
            assertThrows(IllegalStateException.class, () -> index.get(1));
        } finally {
            clearWalSyncFailure(index);
        }
    }

    @Test
    void walRetentionBackpressureSkipsUnsatisfiableSingleActiveSegmentCase() {
        final IndexWalConfiguration wal = IndexWalConfiguration.builder()
                .durability(WalDurabilityMode.ASYNC)
                .segmentSizeBytes(64L * 1024L)
                .maxBytesBeforeForcedCheckpoint(1L).build();
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .identity(identity -> identity.keyClass(Integer.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity.keyTypeDescriptor(tdi))//
                .identity(identity -> identity.valueTypeDescriptor(tds))//
                .identity(identity -> identity.name("wal-retention-single-segment-test"))//
                .segment(segment -> segment.cacheKeyLimit(4))//
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(1))//
                .segment(segment -> segment.chunkKeyLimit(2))//
                .segment(segment -> segment.maxKeys(10))//
                .segment(segment -> segment.cachedSegmentLimit(3))//
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))//
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))//
                .bloomFilter(bloomFilter -> bloomFilter.falsePositiveProbability(0.01D))//
                .io(io -> io.diskBufferSizeBytes(1024))//
                .logging(logging -> logging.contextEnabled(false))//
                .maintenance(maintenance -> maintenance.busyBackoffMillis(1))//
                .maintenance(maintenance -> maintenance.busyTimeoutMillis(200))//
                .filters(filters -> filters.encodingFilters(List.of(new ChunkFilterDoNothing())))//
                .filters(filters -> filters.decodingFilters(List.of(new ChunkFilterDoNothing())))//
                .wal(walBuilder -> walBuilder.configuration(wal))//
                .build();
        if (index != null && !index.wasClosed()) {
            index.close();
        }
        directory = new MemDirectory();
        index = SegmentIndexSessionTestSupport.createStarted(directory, tdi,
                tds, effective(conf));

        index.put(1, "one");
        index.put(2, "two");
        assertEquals("one", index.get(1));
        assertEquals("two", index.get(2));
        assertEquals(SegmentIndexState.READY, index.runtimeMonitoring().snapshot().state());
    }

    private void resetIndex(final int maxKeysInSegment,
            final int segmentWriteCacheKeyLimit) {
        resetIndex(maxKeysInSegment, segmentWriteCacheKeyLimit,
                IndexWalConfiguration.EMPTY);
    }

    private void resetIndex(final int maxKeysInSegment,
            final int segmentWriteCacheKeyLimit, final IndexWalConfiguration wal) {
        if (index != null && !index.wasClosed()) {
            index.close();
        }
        directory = new MemDirectory();
        final IndexConfiguration<Integer, String> conf = buildConf(
                maxKeysInSegment, segmentWriteCacheKeyLimit, wal);
        index = SegmentIndexSessionTestSupport.createStarted(directory, tdi,
                tds, effective(conf));
    }

    private IndexConfiguration<Integer, String> buildConf(
            final int maxKeysInSegment,
            final int segmentWriteCacheKeyLimit, final IndexWalConfiguration wal) {
        return IndexConfiguration.<Integer, String>builder()//
                .identity(identity -> identity.keyClass(Integer.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity.keyTypeDescriptor(tdi))//
                .identity(identity -> identity.valueTypeDescriptor(tds))//
                .identity(identity -> identity.name("test-index"))//
                .segment(segment -> segment.cacheKeyLimit(4))//
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(
                        segmentWriteCacheKeyLimit))//
                .segment(segment -> segment.chunkKeyLimit(2))//
                .segment(segment -> segment.maxKeys(maxKeysInSegment))//
                .segment(segment -> segment.cachedSegmentLimit(3))//
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))//
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))//
                .bloomFilter(bloomFilter -> bloomFilter.falsePositiveProbability(0.01D))//
                .io(io -> io.diskBufferSizeBytes(1024))//
                .logging(logging -> logging.contextEnabled(false))//
                .filters(filters -> filters.encodingFilters(List.of(new ChunkFilterDoNothing())))//
                .filters(filters -> filters.decodingFilters(List.of(new ChunkFilterDoNothing())))//
                .wal(walBuilder -> walBuilder.configuration(wal))//
                .build();
    }

    private static void awaitSegmentCount(
            final SegmentRouteMap<Integer> cache,
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

    private static <K, V> SegmentRouteMap<K> readKeyToSegmentMap(
            final Object index) {
        return SegmentIndexTestAccess.keyToSegmentMap(index);
    }

    @SuppressWarnings("unchecked")
    private static <K, V> SegmentRegistry<K, V> readSegmentRegistry(
            final Object index) {
        return (SegmentRegistry<K, V>) SegmentIndexTestAccess
                .segmentRegistry(index);
    }

    private static void injectWalSyncFailure(final Object index,
            final RuntimeException failure) {
        setWalSyncFailure(index, failure);
    }

    private static void clearWalSyncFailure(final Object index) {
        setWalSyncFailure(index, null);
    }

    private static void setWalSyncFailure(final Object index,
            final RuntimeException failure) {
        try {
            final Object walRuntime = SegmentIndexTestAccess.walRuntime(index);
            final Field syncPolicyField = walRuntime.getClass()
                    .getDeclaredField("syncPolicy");
            syncPolicyField.setAccessible(true);
            final Object syncPolicy = syncPolicyField.get(walRuntime);
            final Field syncFailureField = syncPolicy.getClass()
                    .getDeclaredField("syncFailure");
            syncFailureField.setAccessible(true);
            syncFailureField.set(syncPolicy, failure);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException(
                    "Unable to inject WAL sync failure for test", ex);
        }
    }
}
