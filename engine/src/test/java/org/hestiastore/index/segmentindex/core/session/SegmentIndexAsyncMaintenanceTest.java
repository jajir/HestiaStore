package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

import org.hestiastore.index.OperationResult;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentRuntimeSnapshot;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryFixture;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentIndexAsyncMaintenanceTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();

    private IndexInternalConcurrent<Integer, String> index;

    @Mock
    private Segment<Integer, String> blockingSegment;

    @BeforeEach
    void setUp() {
        index = newIndex();
    }

    @AfterEach
    void tearDown() {
        if (index != null && !index.wasClosed()) {
            index.close();
        }
    }

    @Test
    void flush_waits_for_async_segment_flush() throws Exception {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            index.put(1, "one");
            index.flushAndWait();

            final SegmentId segmentId = readKeyToSegmentMap(index)
                    .findSegmentIdForKey(1);
            final SegmentRegistry<Integer, String> registry = readSegmentRegistry(
                    index);
            final Segment<Integer, String> originalSegment = registry
                    .loadSegment(segmentId).getSegment();
            final CountDownLatch started = new CountDownLatch(1);
            final AtomicReference<SegmentState> stateRef = new AtomicReference<>(
                    SegmentState.READY);
            final Segment<Integer, String> mockedSegment = mockBlockingSegment(
                    started, stateRef, true);
            replaceSegment(registry, segmentId, mockedSegment);

            final Future<?> flushTask = executor.submit(index::flushAndWait);
            assertTrue(started.await(1, TimeUnit.SECONDS));
            assertFalse(flushTask.isDone());
            assertNotNull(stateRef.get());
            stateRef.set(SegmentState.READY);
            flushTask.get(1, TimeUnit.SECONDS);
            replaceSegment(registry, segmentId, originalSegment);
        } finally {
            executor.shutdownNow();
            index.close();
        }
    }

    @Test
    void compact_waits_for_async_segment_compact() throws Exception {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            index.put(1, "one");
            index.flushAndWait();

            final SegmentId segmentId = readKeyToSegmentMap(index)
                    .findSegmentIdForKey(1);
            final SegmentRegistry<Integer, String> registry = readSegmentRegistry(
                    index);
            final Segment<Integer, String> originalSegment = registry
                    .loadSegment(segmentId).getSegment();
            final CountDownLatch started = new CountDownLatch(1);
            final AtomicReference<SegmentState> stateRef = new AtomicReference<>(
                    SegmentState.READY);
            final Segment<Integer, String> mockedSegment = mockBlockingSegment(
                    started, stateRef, false);
            replaceSegment(registry, segmentId, mockedSegment);

            final Future<?> compactTask = executor.submit(index::compactAndWait);
            assertTrue(started.await(1, TimeUnit.SECONDS));
            assertFalse(compactTask.isDone());
            assertNotNull(stateRef.get());
            stateRef.set(SegmentState.READY);
            compactTask.get(1, TimeUnit.SECONDS);
            replaceSegment(registry, segmentId, originalSegment);
        } finally {
            executor.shutdownNow();
            index.close();
        }
    }

    @Test
    void closeStopsAutonomousSplitPolicyLoop() {
        index.put(1, "one");
        index.flushAndWait();

        index.close();
        awaitCondition(() -> index.getState() == SegmentIndexState.CLOSED,
                750L);

        assertEquals(SegmentIndexState.CLOSED, index.getState());
    }

    @Test
    void closeExposesClosingStateWhileBlockedSegmentCloseCompletes()
            throws Exception {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            index.put(1, "one");
            index.flushAndWait();

            final SegmentId segmentId = readKeyToSegmentMap(index)
                    .findSegmentIdForKey(1);
            final SegmentRegistry<Integer, String> registry = readSegmentRegistry(
                    index);
            final Segment<Integer, String> originalSegment = registry
                    .loadSegment(segmentId).getSegment();
            final CountDownLatch started = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);
            final AtomicReference<SegmentState> stateRef = new AtomicReference<>(
                    SegmentState.READY);
            final Segment<Integer, String> mockedSegment = mockCloseBlockingSegment(
                    segmentId, started, release, stateRef);
            replaceSegment(registry, segmentId, mockedSegment);

            try {
                final Future<?> closeTask = executor.submit(index::close);
                assertTrue(started.await(1, TimeUnit.SECONDS));

                awaitCondition(
                        () -> index.getState() == SegmentIndexState.CLOSING,
                        5_000L);
                assertFalse(closeTask.isDone());
                assertEquals(SegmentIndexState.CLOSING, index.metricsSnapshot()
                        .getState());
                assertEquals(SegmentState.MAINTENANCE_RUNNING,
                        stateRef.get());

                release.countDown();
                closeTask.get(5, TimeUnit.SECONDS);
            } finally {
                release.countDown();
                if (!index.wasClosed()) {
                    replaceSegment(registry, segmentId, originalSegment);
                }
            }

            assertEquals(SegmentIndexState.CLOSED, index.getState());
        } finally {
            executor.shutdownNow();
        }
    }

    private IndexInternalConcurrent<Integer, String> newIndex() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        return new IndexInternalConcurrent<>(
                new MemDirectory(),
                tdi, tds, conf, conf.resolveRuntimeConfiguration(),
                ExecutorRegistryFixture.from(conf));
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(tdi)//
                .withValueTypeDescriptor(tds)//
                .withName("async-maintenance-test")//
                .withMaxNumberOfKeysInSegmentCache(10)//
                .withMaxNumberOfKeysInActivePartition(5)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withMaxNumberOfKeysInSegment(100)//
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

    private Segment<Integer, String> mockBlockingSegment(
            final CountDownLatch started,
            final AtomicReference<SegmentState> stateRef,
            final boolean forFlush) {
        when(blockingSegment.getState()).thenAnswer(
                invocation -> stateRef.get());
        lenient().when(blockingSegment.flush()).thenAnswer(invocation -> {
            if (forFlush) {
                stateRef.set(SegmentState.MAINTENANCE_RUNNING);
                started.countDown();
            }
            return OperationResult.ok();
        });
        lenient().when(blockingSegment.compact()).thenAnswer(invocation -> {
            if (!forFlush) {
                stateRef.set(SegmentState.MAINTENANCE_RUNNING);
                started.countDown();
            }
            return OperationResult.ok();
        });
        return blockingSegment;
    }

    @SuppressWarnings("unchecked")
    private Segment<Integer, String> mockCloseBlockingSegment(
            final SegmentId segmentId, final CountDownLatch started,
            final CountDownLatch release,
            final AtomicReference<SegmentState> stateRef) throws Exception {
        final Segment<Integer, String> blockedSegment = mock(Segment.class);
        lenient().when(blockedSegment.getId()).thenReturn(segmentId);
        lenient().when(blockedSegment.getState()).thenAnswer(
                invocation -> stateRef.get());
        lenient().when(blockedSegment.getRuntimeSnapshot()).thenAnswer(
                invocation -> new SegmentRuntimeSnapshot(segmentId,
                        stateRef.get(), 0L, 0L, 0L, 0L, 0, 0, 0L, 0L, 0L, 0L,
                        0L, 0L));
        lenient().when(blockedSegment.flush()).thenReturn(OperationResult.ok());
        lenient().when(blockedSegment.compact()).thenReturn(OperationResult.ok());
        when(blockedSegment.close()).thenAnswer(invocation -> {
            stateRef.set(SegmentState.MAINTENANCE_RUNNING);
            started.countDown();
            if (!release.await(5, TimeUnit.SECONDS)) {
                throw new TimeoutException(
                        "Timed out waiting to release blocked close.");
            }
            stateRef.set(SegmentState.CLOSED);
            return OperationResult.ok();
        });
        return blockedSegment;
    }

    private static void awaitCondition(final Supplier<Boolean> condition,
            final long timeoutMillis) {
        final long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (condition.get()) {
                return;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20L));
            if (Thread.currentThread().isInterrupted()) {
                throw new AssertionError("Interrupted while waiting");
            }
        }
        assertTrue(condition.get(),
                "Condition not reached within " + timeoutMillis + " ms.");
    }

    private static <K, V> void replaceSegment(
            final Object registry, final SegmentId segmentId,
            final Segment<K, V> segment) {
        final Object cache = readCache(registry);
        putCacheEntry(cache, segmentId, segment);
    }

    @SuppressWarnings("unchecked")
    private static <K, V> SegmentRegistry<K, V> readSegmentRegistry(
            final Object index) {
        return (SegmentRegistry<K, V>) SegmentIndexTestAccess
                .segmentRegistry(index);
    }

    @SuppressWarnings("unchecked")
    private static <K, V> KeyToSegmentMap<K> readKeyToSegmentMap(
            final Object index) {
        return SegmentIndexTestAccess.keyToSegmentMap(index);
    }

    @SuppressWarnings("unchecked")
    private static Object readCache(final Object registry) {
        try {
            final Field cacheField = registry.getClass().getDeclaredField(
                    "cache");
            cacheField.setAccessible(true);
            return cacheField.get(registry);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException(
                    "Unable to read registry cache for test", ex);
        }
    }

    private static void putCacheEntry(final Object cache,
            final SegmentId segmentId, final Segment<?, ?> segment) {
        try {
            final Field mapField = cache.getClass().getDeclaredField("map");
            mapField.setAccessible(true);
            @SuppressWarnings("unchecked")
            final java.util.concurrent.ConcurrentHashMap<SegmentId, Object> map = (java.util.concurrent.ConcurrentHashMap<SegmentId, Object>) mapField
                    .get(cache);
            final Class<?> entryClass = Class.forName(
                    "org.hestiastore.index.segmentregistry.SegmentRegistryCache$Entry");
            final var ctor = entryClass.getDeclaredConstructor(long.class);
            ctor.setAccessible(true);
            final Object entry = ctor.newInstance(0L);
            final Field stateField = entryClass.getDeclaredField("state");
            stateField.setAccessible(true);
            final Class<?> stateClass = Class.forName(
                    "org.hestiastore.index.segmentregistry.SegmentRegistryCache$EntryState");
            stateField.set(entry, Enum.valueOf(
                    stateClass.asSubclass(Enum.class), "READY"));
            final Field valueField = entryClass.getDeclaredField("value");
            valueField.setAccessible(true);
            valueField.set(entry, segment);
            map.put(segmentId, entry);
            final Field sizeField = cache.getClass().getDeclaredField("size");
            sizeField.setAccessible(true);
            final java.util.concurrent.atomic.AtomicInteger size = (java.util.concurrent.atomic.AtomicInteger) sizeField
                    .get(cache);
            size.incrementAndGet();
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException(
                    "Unable to update registry cache for test", ex);
        }
    }
}
