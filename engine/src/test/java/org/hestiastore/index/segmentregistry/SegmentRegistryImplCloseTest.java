package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class SegmentRegistryImplCloseTest {

    @Test
    void closeRetriesUntilLoadingEntryBecomesReadyAndRemoved() throws Exception {
        final SegmentRegistryStateMachine gate = new SegmentRegistryStateMachine();
        final SegmentRegistryCache<Integer, String> cache = newCache(4,
                segment -> {
                });
        final SegmentRegistryImpl<Integer, String> registry = newRegistry(cache,
                gate, 1, 2_000);

        final SegmentId segmentId = SegmentId.of(11);
        final SegmentRegistryEntry<Integer, String> entry = new SegmentRegistryEntry<>(
                1L);
        readCacheMap(cache).put(segmentId, entry);

        @SuppressWarnings("unchecked")
        final Segment<Integer, String> segment = Mockito.mock(Segment.class);
        Mockito.when(segment.getState()).thenReturn(SegmentState.READY);

        final ExecutorService closer = Executors.newSingleThreadExecutor();
        try {
            final Future<?> closeFuture = closer.submit(() -> {
                registry.close();
                return null;
            });
            entry.finishLoad(segment);

            closeFuture.get(1, TimeUnit.SECONDS);
            assertTrue(readCacheMap(cache).isEmpty(),
                    "Cache should be empty after close drain");
            assertEquals(SegmentRegistryState.CLOSED, gate.getState());
        } finally {
            closer.shutdownNow();
        }
    }

    @Test
    void closeThrowsWhenCacheCannotDrainBeforeTimeout() {
        final SegmentRegistryStateMachine gate = new SegmentRegistryStateMachine();
        final SegmentRegistryCache<Integer, String> cache = newCache(4,
                segment -> {
                });
        final SegmentRegistryImpl<Integer, String> registry = newRegistry(cache,
                gate, 1, 40);

        final SegmentId segmentId = SegmentId.of(12);
        final SegmentRegistryEntry<Integer, String> loadingEntry = new SegmentRegistryEntry<>(
                1L);
        readCacheMap(cache).put(segmentId, loadingEntry);

        final long startNanos = System.nanoTime();
        final IndexException result = assertThrows(IndexException.class,
                registry::close);
        final long durationMillis = TimeUnit.NANOSECONDS
                .toMillis(System.nanoTime() - startNanos);

        assertTrue(result.getMessage().contains("ERROR"));
        assertEquals(SegmentRegistryState.ERROR, gate.getState());
        assertTrue(durationMillis >= 30L,
                "Close should retry until timeout budget is consumed");
    }

    private static SegmentRegistryImpl<Integer, String> newRegistry(
            final SegmentRegistryCache<Integer, String> cache,
            final SegmentRegistryStateMachine gate, final int backoffMillis,
            final int timeoutMillis) {
        final Directory directory = new MemDirectory();
        final SegmentRegistryFileSystem fs = new SegmentRegistryFileSystem(
                directory);
        final AtomicInteger counter = new AtomicInteger();
        final SegmentIdAllocator allocator = () -> SegmentId
                .of(counter.getAndIncrement());
        final BusyRetryPolicy closeRetryPolicy = new BusyRetryPolicy(
                backoffMillis, timeoutMillis);
        @SuppressWarnings("unchecked")
        final PreparedSegmentWriterFactory<Integer, String> writerFactory = Mockito
                .mock(PreparedSegmentWriterFactory.class);
        final SegmentRuntimeTuner runtimeTuner = Mockito
                .mock(SegmentRuntimeTuner.class);
        final BusyRetryPolicy blockingRetryPolicy = new BusyRetryPolicy(
                backoffMillis, timeoutMillis);
        return new SegmentRegistryImpl<>(allocator, fs, cache, closeRetryPolicy,
                gate, writerFactory, runtimeTuner, blockingRetryPolicy, false);
    }

    private static SegmentRegistryCache<Integer, String> newCache(
            final int limit,
            final Consumer<Segment<Integer, String>> unloader) {
        @SuppressWarnings("unchecked")
        final SegmentLoadCloseOperations<Integer, String> segmentOperations = Mockito
                .mock(SegmentLoadCloseOperations.class);
        Mockito.when(segmentOperations.loadSegment(Mockito.any(SegmentId.class)))
                .thenThrow(new IllegalStateException(
                        "Loader should not be used"));
        Mockito.doAnswer(invocation -> {
            unloader.accept(invocation.getArgument(0));
            return null;
        }).when(segmentOperations)
                .closeSegmentIfNeeded(Mockito.<Segment<Integer, String>>any());
        final SegmentUnloadEligibility unloadEligibility = Mockito
                .mock(SegmentUnloadEligibility.class);
        Mockito.when(unloadEligibility.canUnload(Mockito.any()))
                .thenReturn(true);
        return new SegmentRegistryCache<>(limit, segmentOperations,
                unloadEligibility, Runnable::run);
    }

    @SuppressWarnings("unchecked")
    private static Map<SegmentId, SegmentRegistryEntry<Integer, String>> readCacheMap(
            final SegmentRegistryCache<Integer, String> cache) {
        try {
            final Field mapField = SegmentRegistryCache.class
                    .getDeclaredField("map");
            mapField.setAccessible(true);
            return (Map<SegmentId, SegmentRegistryEntry<Integer, String>>) mapField
                    .get(cache);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException("Unable to access cache map", ex);
        }
    }
}
