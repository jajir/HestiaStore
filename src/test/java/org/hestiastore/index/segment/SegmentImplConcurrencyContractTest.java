package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.hestiastore.index.segment.SegmentTestHelper.closeAndAwait;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
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
        final Segment<Integer, String> segment = newSegment(2);
        try {
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
        } finally {
            closeAndAwait(segment);
        }
    }

    @Test
    void exclusiveAccess_invalidates_failFast_iterators() {
        final Segment<Integer, String> segment = newSegment(2);
        try {
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
        } finally {
            closeAndAwait(segment);
        }
    }

    @Test
    void flush_invalidates_failFast_iterators() {
        final Segment<Integer, String> segment = newSegment(2);
        try {
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
        } finally {
            closeAndAwait(segment);
        }
    }

    @Test
    void flush_allows_put_and_get_during_maintenance() {
        final CapturingExecutor executor = new CapturingExecutor();
        final Segment<Integer, String> segment = newSegment(4, executor);
        try {
            assertEquals(SegmentResultStatus.OK,
                    segment.put(1, "a").getStatus());

            final SegmentResult<CompletionStage<Void>> result = segment.flush();
            assertEquals(SegmentResultStatus.OK, result.getStatus());
            assertEquals(SegmentState.MAINTENANCE_RUNNING, segment.getState());
            assertNotNull(result.getValue());
            assertTrue(executor.hasTask());
            assertFalse(result.getValue().toCompletableFuture().isDone());

            assertEquals(SegmentResultStatus.OK,
                    segment.put(2, "b").getStatus());
            final SegmentResult<String> read = segment.get(2);
            assertEquals(SegmentResultStatus.OK, read.getStatus());
            assertEquals("b", read.getValue());

            executor.runTask();

            assertEquals(SegmentState.READY, segment.getState());
            assertTrue(result.getValue().toCompletableFuture().isDone());
            assertEquals("a", segment.get(1).getValue());
            assertEquals("b", segment.get(2).getValue());
        } finally {
            closeAndDrainExecutor(segment, executor);
        }
    }

    @Test
    void put_returns_busy_when_write_cache_full() {
        final Segment<Integer, String> segment = newSegment(1);
        try {
            assertEquals(SegmentResultStatus.OK,
                    segment.put(1, "a").getStatus());
            assertEquals(SegmentResultStatus.BUSY,
                    segment.put(2, "b").getStatus());
        } finally {
            closeAndAwait(segment);
        }
    }

    @Test
    void compact_allows_put_and_get_during_maintenance() {
        final CapturingExecutor executor = new CapturingExecutor();
        final Segment<Integer, String> segment = newSegment(4, executor);
        try {
            assertEquals(SegmentResultStatus.OK,
                    segment.put(1, "a").getStatus());

            final SegmentResult<CompletionStage<Void>> result = segment
                    .compact();
            assertEquals(SegmentResultStatus.OK, result.getStatus());
            assertEquals(SegmentState.MAINTENANCE_RUNNING, segment.getState());
            assertNotNull(result.getValue());
            assertTrue(executor.hasTask());
            assertFalse(result.getValue().toCompletableFuture().isDone());

            assertEquals(SegmentResultStatus.OK,
                    segment.put(2, "b").getStatus());
            final SegmentResult<String> read = segment.get(2);
            assertEquals(SegmentResultStatus.OK, read.getStatus());
            assertEquals("b", read.getValue());

            executor.runTask();

            assertEquals(SegmentState.READY, segment.getState());
            assertTrue(result.getValue().toCompletableFuture().isDone());
            assertEquals("a", segment.get(1).getValue());
            assertEquals("b", segment.get(2).getValue());
        } finally {
            closeAndDrainExecutor(segment, executor);
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void concurrent_get_and_put_succeed() throws Exception {
        final Segment<Integer, String> segment = newSegment(128);
        try {
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
        } finally {
            closeAndAwait(segment);
        }
    }

    private Segment<Integer, String> newSegment(final int writeCacheSize) {
        return newSegment(writeCacheSize, null);
    }

    private Segment<Integer, String> newSegment(final int writeCacheSize,
            final Executor maintenanceExecutor) {
        final SegmentBuilder<Integer, String> builder = Segment
                .<Integer, String>builder(
                        AsyncDirectoryAdapter.wrap(new MemDirectory()))
                .withId(SegmentId.of(1))
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorShortString())
                .withMaxNumberOfKeysInSegmentWriteCache(writeCacheSize)
                .withMaxNumberOfKeysInSegmentCache(8)
                .withMaxNumberOfKeysInSegmentChunk(2)
                .withBloomFilterIndexSizeInBytes(0)
                .withSegmentMaintenanceAutoEnabled(false)
                .withEncodingChunkFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingChunkFilters(List.of(new ChunkFilterDoNothing()));
        if (maintenanceExecutor != null) {
            builder.withMaintenanceExecutor(maintenanceExecutor);
        }
        return builder.build();
    }

    private static void closeAndDrainExecutor(final Segment<?, ?> segment,
            final CapturingExecutor executor) {
        if (segment == null) {
            return;
        }
        final SegmentResult<Void> closeResult = segment.close();
        if (closeResult.getStatus() == SegmentResultStatus.BUSY
                && executor.hasTask()) {
            executor.runTask();
            segment.close();
        }
        if (executor.hasTask()) {
            executor.runTask();
        }
        if (segment.getState() != SegmentState.CLOSED) {
            throw new AssertionError("Segment did not close.");
        }
    }

    private static final class CapturingExecutor implements Executor {

        private Runnable task;

        @Override
        public void execute(final Runnable command) {
            this.task = command;
        }

        boolean hasTask() {
            return task != null;
        }

        void runTask() {
            if (task != null) {
                final Runnable toRun = task;
                task = null;
                toRun.run();
            }
        }
    }
}
