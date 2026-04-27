package org.hestiastore.index.segment;

import org.hestiastore.index.OperationStatus;
import org.hestiastore.index.OperationResult;
import static org.hestiastore.index.segment.SegmentTestHelper.closeAndAssertClosed;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class SegmentImplConcurrencyContractTest {

    @Test
    void exclusiveAccess_blocks_operations_until_closed() {
        final Segment<Integer, String> segment = newSegment(2);
        try {
            assertEquals(OperationStatus.OK,
                    segment.put(1, "a").getStatus());
            final OperationResult<EntryIterator<Integer, String>> exclusive = segment
                    .openIterator(SegmentIteratorIsolation.FULL_ISOLATION);
            assertEquals(OperationStatus.OK, exclusive.getStatus());

            assertEquals(OperationStatus.BUSY,
                    segment.put(2, "b").getStatus());
            assertEquals(OperationStatus.BUSY, segment.get(1).getStatus());
            assertEquals(OperationStatus.BUSY,
                    segment.flush().getStatus());
            assertEquals(OperationStatus.BUSY,
                    segment.compact().getStatus());
            assertEquals(OperationStatus.BUSY,
                    segment.openIterator().getStatus());

            exclusive.getValue().close();

            assertEquals(OperationStatus.OK,
                    segment.put(2, "b").getStatus());
            assertEquals(OperationStatus.OK, segment.get(1).getStatus());
        } finally {
            closeAndAssertClosed(segment);
        }
    }

    @Test
    void exclusiveAccess_invalidates_failFast_iterators() {
        final Segment<Integer, String> segment = newSegment(2);
        try {
            assertEquals(OperationStatus.OK,
                    segment.put(1, "a").getStatus());
            assertEquals(OperationStatus.OK,
                    segment.put(2, "b").getStatus());
            final OperationResult<EntryIterator<Integer, String>> failFastResult = segment
                    .openIterator();
            assertEquals(OperationStatus.OK, failFastResult.getStatus());
            final EntryIterator<Integer, String> failFast = failFastResult
                    .getValue();
            assertTrue(failFast.hasNext());

            final OperationResult<EntryIterator<Integer, String>> exclusive = segment
                    .openIterator(SegmentIteratorIsolation.FULL_ISOLATION);
            assertEquals(OperationStatus.OK, exclusive.getStatus());

            assertFalse(failFast.hasNext());

            exclusive.getValue().close();
            failFast.close();
        } finally {
            closeAndAssertClosed(segment);
        }
    }

    @Test
    void flush_invalidates_failFast_iterators() {
        final Segment<Integer, String> segment = newSegment(2);
        try {
            assertEquals(OperationStatus.OK,
                    segment.put(1, "a").getStatus());
            final OperationResult<EntryIterator<Integer, String>> failFastResult = segment
                    .openIterator();
            assertEquals(OperationStatus.OK, failFastResult.getStatus());
            final EntryIterator<Integer, String> failFast = failFastResult
                    .getValue();
            assertTrue(failFast.hasNext());

            assertEquals(OperationStatus.OK,
                    segment.flush().getStatus());

            assertFalse(failFast.hasNext());
            failFast.close();
        } finally {
            closeAndAssertClosed(segment);
        }
    }

    @Test
    void flush_allows_put_and_get_during_maintenance() {
        final CapturingExecutor executor = new CapturingExecutor();
        final Segment<Integer, String> segment = newSegment(4, executor);
        try {
            assertEquals(OperationStatus.OK,
                    segment.put(1, "a").getStatus());

            final OperationResult<Void> result = segment.flush();
            assertEquals(OperationStatus.OK, result.getStatus());
            assertEquals(SegmentState.MAINTENANCE_RUNNING, segment.getState());
            assertTrue(executor.hasTask());

            assertEquals(OperationStatus.OK,
                    segment.put(2, "b").getStatus());
            final OperationResult<String> read = segment.get(2);
            assertEquals(OperationStatus.OK, read.getStatus());
            assertEquals("b", read.getValue());

            executor.runTask();

            assertEquals(SegmentState.READY, segment.getState());
            assertEquals("a", segment.get(1).getValue());
            assertEquals("b", segment.get(2).getValue());
        } finally {
            closeAfterMaintenanceIfNeeded(segment, executor);
        }
    }

    @Test
    void put_returns_busy_when_write_cache_full() {
        final Segment<Integer, String> segment = newSegment(1);
        try {
            assertEquals(OperationStatus.OK,
                    segment.put(1, "a").getStatus());
            assertEquals(OperationStatus.BUSY,
                    segment.put(2, "b").getStatus());
        } finally {
            closeAndAssertClosed(segment);
        }
    }

    @Test
    void compact_allows_put_and_get_during_maintenance() {
        final CapturingExecutor executor = new CapturingExecutor();
        final Segment<Integer, String> segment = newSegment(4, executor);
        try {
            assertEquals(OperationStatus.OK,
                    segment.put(1, "a").getStatus());

            final OperationResult<Void> result = segment.compact();
            assertEquals(OperationStatus.OK, result.getStatus());
            assertEquals(SegmentState.MAINTENANCE_RUNNING, segment.getState());
            assertTrue(executor.hasTask());

            assertEquals(OperationStatus.OK,
                    segment.put(2, "b").getStatus());
            final OperationResult<String> read = segment.get(2);
            assertEquals(OperationStatus.OK, read.getStatus());
            assertEquals("b", read.getValue());

            executor.runTask();

            assertEquals(SegmentState.READY, segment.getState());
            assertEquals("a", segment.get(1).getValue());
            assertEquals("b", segment.get(2).getValue());
        } finally {
            closeAfterMaintenanceIfNeeded(segment, executor);
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
                        final OperationResult<Void> result = segment.put(i,
                                "v" + i);
                        if (result.getStatus() != OperationStatus.OK) {
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
                        final OperationResult<String> result = segment.get(i);
                        if (result.getStatus() != OperationStatus.OK) {
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
                final OperationResult<String> result = segment.get(i);
                assertEquals(OperationStatus.OK, result.getStatus());
                assertEquals("v" + i, result.getValue());
            }
        } finally {
            closeAndAssertClosed(segment);
        }
    }

    private Segment<Integer, String> newSegment(final int writeCacheSize) {
        return newSegment(writeCacheSize, null);
    }

    private Segment<Integer, String> newSegment(final int writeCacheSize,
            final Executor maintenanceExecutor) {
        final SegmentBuilder<Integer, String> builder = Segment
                .<Integer, String>builder(
                        new MemDirectory())
                .withId(SegmentId.of(1))
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorShortString())
                .withMaxNumberOfKeysInSegmentWriteCache(writeCacheSize)
                .withMaxNumberOfKeysInSegmentCache(8)
                .withMaxNumberOfKeysInSegmentChunk(2)
                .withBloomFilterIndexSizeInBytes(0)
                .withMaintenancePolicy(SegmentMaintenancePolicy.none())
                .withEncodingChunkFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingChunkFilters(List.of(new ChunkFilterDoNothing()));
        if (maintenanceExecutor != null) {
            builder.withMaintenanceExecutor(maintenanceExecutor);
        }
        return builder.build().getValue();
    }

    private static void closeAfterMaintenanceIfNeeded(
            final Segment<?, ?> segment,
            final CapturingExecutor executor) {
        if (segment == null) {
            return;
        }
        final OperationResult<Void> closeResult = segment.close();
        if (closeResult.getStatus() == OperationStatus.BUSY
                && executor.hasTask()) {
            executor.runTask();
            segment.close();
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
