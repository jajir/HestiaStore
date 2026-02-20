package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class SegmentRegistryImplCloseTest {

    @Test
    void closeRetriesUntilLoadingEntryBecomesReadyAndRemoved() throws Exception {
        final SegmentRegistryStateMachine gate = new SegmentRegistryStateMachine();
        final SegmentRegistryCache<SegmentId, Segment<Integer, String>> cache = new SegmentRegistryCache<>(
                4, id -> {
                    throw new IllegalStateException("Loader should not be used");
                }, segment -> {
                });
        final SegmentRegistryImpl<Integer, String> registry = newRegistry(cache,
                gate, 1, 2_000);

        final SegmentId segmentId = SegmentId.of(11);
        final SegmentRegistryCache.Entry<Segment<Integer, String>> entry = new SegmentRegistryCache.Entry<>(
                1L);
        readCacheMap(cache).put(segmentId, entry);

        @SuppressWarnings("unchecked")
        final Segment<Integer, String> segment = Mockito.mock(Segment.class);
        Mockito.when(segment.getState()).thenReturn(SegmentState.READY);

        final ExecutorService closer = Executors.newSingleThreadExecutor();
        try {
            final Future<SegmentRegistryResult<Void>> closeFuture = closer
                    .submit(registry::close);
            Thread.sleep(30L);
            entry.finishLoad(segment);

            final SegmentRegistryResult<Void> result = closeFuture.get(1,
                    TimeUnit.SECONDS);
            assertSame(SegmentRegistryResultStatus.OK, result.getStatus());
            assertTrue(readCacheMap(cache).isEmpty(),
                    "Cache should be empty after close drain");
            assertEquals(SegmentRegistryState.CLOSED, gate.getState());
        } finally {
            closer.shutdownNow();
        }
    }

    @Test
    void closeReturnsErrorWhenCacheCannotDrainBeforeTimeout() throws Exception {
        final SegmentRegistryStateMachine gate = new SegmentRegistryStateMachine();
        final SegmentRegistryCache<SegmentId, Segment<Integer, String>> cache = new SegmentRegistryCache<>(
                4, id -> {
                    throw new IllegalStateException("Loader should not be used");
                }, segment -> {
                });
        final SegmentRegistryImpl<Integer, String> registry = newRegistry(cache,
                gate, 1, 40);

        final SegmentId segmentId = SegmentId.of(12);
        final SegmentRegistryCache.Entry<Segment<Integer, String>> loadingEntry = new SegmentRegistryCache.Entry<>(
                1L);
        readCacheMap(cache).put(segmentId, loadingEntry);

        final long startNanos = System.nanoTime();
        final SegmentRegistryResult<Void> result = registry.close();
        final long durationMillis = TimeUnit.NANOSECONDS
                .toMillis(System.nanoTime() - startNanos);

        assertSame(SegmentRegistryResultStatus.ERROR, result.getStatus());
        assertEquals(SegmentRegistryState.ERROR, gate.getState());
        assertTrue(durationMillis >= 30L,
                "Close should retry until timeout budget is consumed");
    }

    private static SegmentRegistryImpl<Integer, String> newRegistry(
            final SegmentRegistryCache<SegmentId, Segment<Integer, String>> cache,
            final SegmentRegistryStateMachine gate, final int backoffMillis,
            final int timeoutMillis) {
        final Directory directory = new MemDirectory();
        final SegmentRegistryFileSystem fs = new SegmentRegistryFileSystem(
                directory);
        final AtomicInteger counter = new AtomicInteger();
        final SegmentIdAllocator allocator = () -> SegmentId
                .of(counter.getAndIncrement());
        final IndexRetryPolicy closeRetryPolicy = new IndexRetryPolicy(
                backoffMillis, timeoutMillis);
        return new SegmentRegistryImpl<>(allocator, fs, cache, closeRetryPolicy,
                gate);
    }

    @SuppressWarnings("unchecked")
    private static Map<SegmentId, SegmentRegistryCache.Entry<Segment<Integer, String>>> readCacheMap(
            final SegmentRegistryCache<SegmentId, Segment<Integer, String>> cache) {
        try {
            final Field mapField = SegmentRegistryCache.class
                    .getDeclaredField("map");
            mapField.setAccessible(true);
            return (Map<SegmentId, SegmentRegistryCache.Entry<Segment<Integer, String>>>) mapField
                    .get(cache);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException("Unable to access cache map", ex);
        }
    }
}
