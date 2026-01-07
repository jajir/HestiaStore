package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segmentasync.SegmentAsync;
import org.junit.jupiter.api.Test;

class SegmentIndexAsyncMaintenanceTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();

    @Test
    void flush_waits_for_async_segment_flush() throws Exception {
        final IndexInternalSynchronized<Integer, String> index = newIndex();
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            index.put(1, "one");

            final SegmentId segmentId = readKeySegmentCache(index)
                    .findSegmentId(1);
            final SegmentRegistry<Integer, String> registry = readSegmentRegistry(
                    index);
            final SegmentAsync<Integer, String> originalSegment = registry
                    .getSegment(segmentId);
            final CountDownLatch started = new CountDownLatch(1);
            final AtomicReference<CompletableFuture<SegmentResult<Void>>> futureRef = new AtomicReference<>();
            final SegmentAsync<Integer, String> blockingSegment = mockBlockingSegment(
                    segmentId, started, futureRef, true);
            replaceSegment(registry, segmentId, blockingSegment);

            final Future<?> flushTask = executor.submit(index::flush);
            assertTrue(started.await(1, TimeUnit.SECONDS));
            assertFalse(flushTask.isDone());
            assertNotNull(futureRef.get());
            futureRef.get().complete(SegmentResult.ok());
            flushTask.get(1, TimeUnit.SECONDS);
            replaceSegment(registry, segmentId, originalSegment);
        } finally {
            executor.shutdownNow();
            index.close();
        }
    }

    @Test
    void compact_waits_for_async_segment_compact() throws Exception {
        final IndexInternalSynchronized<Integer, String> index = newIndex();
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            index.put(1, "one");

            final SegmentId segmentId = readKeySegmentCache(index)
                    .findSegmentId(1);
            final SegmentRegistry<Integer, String> registry = readSegmentRegistry(
                    index);
            final SegmentAsync<Integer, String> originalSegment = registry
                    .getSegment(segmentId);
            final CountDownLatch started = new CountDownLatch(1);
            final AtomicReference<CompletableFuture<SegmentResult<Void>>> futureRef = new AtomicReference<>();
            final SegmentAsync<Integer, String> blockingSegment = mockBlockingSegment(
                    segmentId, started, futureRef, false);
            replaceSegment(registry, segmentId, blockingSegment);

            final Future<?> compactTask = executor.submit(index::compact);
            assertTrue(started.await(1, TimeUnit.SECONDS));
            assertFalse(compactTask.isDone());
            assertNotNull(futureRef.get());
            futureRef.get().complete(SegmentResult.ok());
            compactTask.get(1, TimeUnit.SECONDS);
            replaceSegment(registry, segmentId, originalSegment);
        } finally {
            executor.shutdownNow();
            index.close();
        }
    }

    private IndexInternalSynchronized<Integer, String> newIndex() {
        return new IndexInternalSynchronized<>(
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

    private SegmentAsync<Integer, String> mockBlockingSegment(
            final SegmentId segmentId, final CountDownLatch started,
            final AtomicReference<CompletableFuture<SegmentResult<Void>>> futureRef,
            final boolean forFlush) {
        @SuppressWarnings("unchecked")
        final SegmentAsync<Integer, String> segment = (SegmentAsync<Integer, String>) mock(
                SegmentAsync.class);
        when(segment.wasClosed()).thenReturn(false);
        when(segment.getId()).thenReturn(segmentId);
        if (forFlush) {
            when(segment.flushAsync()).thenAnswer(invocation -> {
                final CompletableFuture<SegmentResult<Void>> future = new CompletableFuture<>();
                futureRef.set(future);
                started.countDown();
                return future;
            });
            when(segment.compactAsync())
                    .thenReturn(
                            CompletableFuture.completedFuture(SegmentResult.ok()));
        } else {
            when(segment.compactAsync()).thenAnswer(invocation -> {
                final CompletableFuture<SegmentResult<Void>> future = new CompletableFuture<>();
                futureRef.set(future);
                started.countDown();
                return future;
            });
            when(segment.flushAsync())
                    .thenReturn(
                            CompletableFuture.completedFuture(SegmentResult.ok()));
        }
        return segment;
    }

    private static <K, V> void replaceSegment(final SegmentRegistry<K, V> registry,
            final SegmentId segmentId, final SegmentAsync<K, V> segment) {
        final Map<SegmentId, SegmentAsync<K, V>> segments = readSegmentsMap(
                registry);
        registry.executeWithRegistryLock(
                () -> segments.put(segmentId, segment));
    }

    @SuppressWarnings("unchecked")
    private static <K, V> SegmentRegistry<K, V> readSegmentRegistry(
            final SegmentIndexImpl<K, V> index) {
        try {
            final Field field = SegmentIndexImpl.class
                    .getDeclaredField("segmentRegistry");
            field.setAccessible(true);
            return (SegmentRegistry<K, V>) field.get(index);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException(
                    "Unable to read segmentRegistry for test", ex);
        }
    }

    @SuppressWarnings("unchecked")
    private static <K, V> KeySegmentCache<K> readKeySegmentCache(
            final SegmentIndexImpl<K, V> index) {
        try {
            final Field field = SegmentIndexImpl.class
                    .getDeclaredField("keySegmentCache");
            field.setAccessible(true);
            return (KeySegmentCache<K>) field.get(index);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException(
                    "Unable to read keySegmentCache for test", ex);
        }
    }

    @SuppressWarnings("unchecked")
    private static <K, V> Map<SegmentId, SegmentAsync<K, V>> readSegmentsMap(
            final SegmentRegistry<K, V> registry) {
        try {
            final Field field = SegmentRegistry.class
                    .getDeclaredField("segments");
            field.setAccessible(true);
            return (Map<SegmentId, SegmentAsync<K, V>>) field.get(registry);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException(
                    "Unable to read segments map for test", ex);
        }
    }
}
