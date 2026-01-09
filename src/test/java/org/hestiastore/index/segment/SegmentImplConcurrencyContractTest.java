package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class SegmentImplConcurrencyContractTest {

    @Test
    void exclusiveAccess_blocks_operations_until_closed() {
        try (Segment<Integer, String> segment = newSegment(2)) {
            assertEquals(SegmentResultStatus.OK,
                    segment.put(1, "a").getStatus());
            final SegmentResult<EntryIterator<Integer, String>> exclusive = segment
                    .openIterator(SegmentIteratorIsolation.FULL_ISOLATION);
            assertEquals(SegmentResultStatus.OK, exclusive.getStatus());

            assertEquals(SegmentResultStatus.BUSY,
                    segment.put(2, "b").getStatus());
            assertEquals(SegmentResultStatus.BUSY, segment.get(1).getStatus());
            assertEquals(SegmentResultStatus.BUSY,
                    segment.flush().getStatus());
            assertEquals(SegmentResultStatus.BUSY,
                    segment.compact().getStatus());
            assertEquals(SegmentResultStatus.BUSY,
                    segment.openIterator().getStatus());

            exclusive.getValue().close();

            assertEquals(SegmentResultStatus.OK,
                    segment.put(2, "b").getStatus());
            assertEquals(SegmentResultStatus.OK, segment.get(1).getStatus());
        }
    }

    @Test
    void exclusiveAccess_invalidates_failFast_iterators() {
        try (Segment<Integer, String> segment = newSegment(2)) {
            assertEquals(SegmentResultStatus.OK,
                    segment.put(1, "a").getStatus());
            assertEquals(SegmentResultStatus.OK,
                    segment.put(2, "b").getStatus());
            final SegmentResult<EntryIterator<Integer, String>> failFastResult = segment
                    .openIterator();
            assertEquals(SegmentResultStatus.OK, failFastResult.getStatus());
            final EntryIterator<Integer, String> failFast = failFastResult
                    .getValue();
            assertTrue(failFast.hasNext());

            final SegmentResult<EntryIterator<Integer, String>> exclusive = segment
                    .openIterator(SegmentIteratorIsolation.FULL_ISOLATION);
            assertEquals(SegmentResultStatus.OK, exclusive.getStatus());

            assertFalse(failFast.hasNext());

            exclusive.getValue().close();
            failFast.close();
        }
    }

    @Test
    void flush_invalidates_failFast_iterators() {
        try (Segment<Integer, String> segment = newSegment(2)) {
            assertEquals(SegmentResultStatus.OK,
                    segment.put(1, "a").getStatus());
            final SegmentResult<EntryIterator<Integer, String>> failFastResult = segment
                    .openIterator();
            assertEquals(SegmentResultStatus.OK, failFastResult.getStatus());
            final EntryIterator<Integer, String> failFast = failFastResult
                    .getValue();
            assertTrue(failFast.hasNext());

            assertEquals(SegmentResultStatus.OK,
                    segment.flush().getStatus());

            assertFalse(failFast.hasNext());
            failFast.close();
        }
    }

    @Test
    void put_returns_busy_when_write_cache_full() {
        try (Segment<Integer, String> segment = newSegment(1)) {
            assertEquals(SegmentResultStatus.OK,
                    segment.put(1, "a").getStatus());
            assertEquals(SegmentResultStatus.BUSY,
                    segment.put(2, "b").getStatus());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void concurrent_get_and_put_succeed() throws Exception {
        try (Segment<Integer, String> segment = newSegment(128)) {
            final int items = 50;
            final ExecutorService executor = Executors.newFixedThreadPool(2);
            final CountDownLatch start = new CountDownLatch(1);
            try {
                final Future<?> writer = executor.submit(() -> {
                    try {
                        start.await();
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(
                                "Writer interrupted before start", e);
                    }
                    for (int i = 0; i < items; i++) {
                        final SegmentResult<Void> result = segment.put(i,
                                "v" + i);
                        if (result.getStatus() != SegmentResultStatus.OK) {
                            throw new IllegalStateException(
                                    "Put returned " + result.getStatus());
                        }
                    }
                    return null;
                });
                final Future<?> reader = executor.submit(() -> {
                    try {
                        start.await();
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(
                                "Reader interrupted before start", e);
                    }
                    for (int i = 0; i < items; i++) {
                        final SegmentResult<String> result = segment.get(i);
                        if (result.getStatus() != SegmentResultStatus.OK) {
                            throw new IllegalStateException(
                                    "Get returned " + result.getStatus());
                        }
                    }
                    return null;
                });
                start.countDown();
                writer.get(2, TimeUnit.SECONDS);
                reader.get(2, TimeUnit.SECONDS);
            } finally {
                executor.shutdownNow();
            }

            for (int i = 0; i < items; i++) {
                final SegmentResult<String> result = segment.get(i);
                assertEquals(SegmentResultStatus.OK, result.getStatus());
                assertEquals("v" + i, result.getValue());
            }
        }
    }

    private Segment<Integer, String> newSegment(final int writeCacheSize) {
        return Segment.<Integer, String>builder()//
                .withAsyncDirectory(AsyncDirectoryAdapter.wrap(new MemDirectory()))//
                .withId(SegmentId.of(1))//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorShortString())//
                .withMaxNumberOfKeysInSegmentWriteCache(writeCacheSize)//
                .withMaxNumberOfKeysInSegmentCache(8)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withBloomFilterIndexSizeInBytes(0)//
                .withEncodingChunkFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingChunkFilters(List.of(new ChunkFilterDoNothing()))//
                .build();
    }
}
