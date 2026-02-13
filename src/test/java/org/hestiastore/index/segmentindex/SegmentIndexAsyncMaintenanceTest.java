package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentregistry.SegmentHandler;
import org.hestiastore.index.segmentregistry.SegmentRegistryCache;
import org.hestiastore.index.segmentregistry.SegmentRegistryImpl;
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

            final SegmentId segmentId = readKeyToSegmentMap(index)
                    .findSegmentId(1);
            final SegmentRegistryImpl<Integer, String> registry = readSegmentRegistry(
                    index);
            final Segment<Integer, String> originalSegment = registry
                    .getSegment(segmentId).getSegment().orElse(null);
            final CountDownLatch started = new CountDownLatch(1);
            final AtomicReference<SegmentState> stateRef = new AtomicReference<>(
                    SegmentState.READY);
            final Segment<Integer, String> blockingSegment = mockBlockingSegment(
                    segmentId, started, stateRef, true);
            replaceSegment(registry, segmentId, blockingSegment);

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

            final SegmentId segmentId = readKeyToSegmentMap(index)
                    .findSegmentId(1);
            final SegmentRegistryImpl<Integer, String> registry = readSegmentRegistry(
                    index);
            final Segment<Integer, String> originalSegment = registry
                    .getSegment(segmentId).getSegment().orElse(null);
            final CountDownLatch started = new CountDownLatch(1);
            final AtomicReference<SegmentState> stateRef = new AtomicReference<>(
                    SegmentState.READY);
            final Segment<Integer, String> blockingSegment = mockBlockingSegment(
                    segmentId, started, stateRef, false);
            replaceSegment(registry, segmentId, blockingSegment);

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

    private IndexInternalConcurrent<Integer, String> newIndex() {
        return new IndexInternalConcurrent<>(
                AsyncDirectoryAdapter.wrap(new MemDirectory()),
                tdi, tds, buildConf());
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(tdi)//
                .withValueTypeDescriptor(tds)//
                .withName("async-maintenance-test")//
                .withMaxNumberOfKeysInSegmentCache(10)//
                .withMaxNumberOfKeysInSegmentWriteCache(5)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withMaxNumberOfKeysInCache(10)//
                .withMaxNumberOfKeysInSegment(100)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withContextLoggingEnabled(false)//
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withNumberOfCpuThreads(1)//
                .withNumberOfIoThreads(1)//
                .build();
    }

    private Segment<Integer, String> mockBlockingSegment(
            final SegmentId segmentId, final CountDownLatch started,
            final AtomicReference<SegmentState> stateRef,
            final boolean forFlush) {
        when(blockingSegment.getState()).thenAnswer(
                invocation -> stateRef.get());
        if (forFlush) {
            when(blockingSegment.flush()).thenAnswer(invocation -> {
                stateRef.set(SegmentState.MAINTENANCE_RUNNING);
                started.countDown();
                return SegmentResult.ok();
            });
        } else {
            when(blockingSegment.compact()).thenAnswer(invocation -> {
                stateRef.set(SegmentState.MAINTENANCE_RUNNING);
                started.countDown();
                return SegmentResult.ok();
            });
        }
        return blockingSegment;
    }

    private static <K, V> void replaceSegment(
            final SegmentRegistryImpl<K, V> registry, final SegmentId segmentId,
            final Segment<K, V> segment) {
        final SegmentRegistryCache<K, V> cache = readCache(registry);
        putCacheEntry(cache, segmentId, new SegmentHandler<>(segment));
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
    private static <K, V> SegmentRegistryCache<K, V> readCache(
            final SegmentRegistryImpl<K, V> registry) {
        try {
            final Field cacheField = SegmentRegistryImpl.class
                    .getDeclaredField("cache");
            cacheField.setAccessible(true);
            return (SegmentRegistryCache<K, V>) cacheField.get(registry);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException(
                    "Unable to read registry cache for test", ex);
        }
    }

    private static void putCacheEntry(final Object cache,
            final SegmentId segmentId, final SegmentHandler<?, ?> handler) {
        try {
            final Field mapField = cache.getClass().getDeclaredField("map");
            mapField.setAccessible(true);
            @SuppressWarnings("unchecked")
            final java.util.concurrent.ConcurrentHashMap<SegmentId, Object> map = (java.util.concurrent.ConcurrentHashMap<SegmentId, Object>) mapField
                    .get(cache);
            final Class<?> entryClass = Class.forName(
                    SegmentRegistryCache.class.getName() + "$Entry");
            final var ctor = entryClass.getDeclaredConstructor();
            ctor.setAccessible(true);
            final Object entry = ctor.newInstance();
            final Field stateField = entryClass.getDeclaredField("state");
            stateField.setAccessible(true);
            final Class<?> stateClass = Class.forName(
                    SegmentRegistryCache.class.getName() + "$EntryState");
            stateField.set(entry, Enum.valueOf(
                    stateClass.asSubclass(Enum.class), "READY"));
            final Field valueField = entryClass.getDeclaredField("value");
            valueField.setAccessible(true);
            valueField.set(entry, handler);
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
