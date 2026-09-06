package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.datatype.NullValue.NULL;
import static org.hestiastore.index.senku.internal.LargeFileTestSupport.DATA_BLOCK_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.chunkentryfile.KeyPageCodecs;
import org.hestiastore.index.chunkentryfile.LongKeyPageReader;
import org.hestiastore.index.chunkstore.Compression;
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorNull;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.MemFileReader;
import org.junit.jupiter.api.Test;

class SenkuFlushPagePipelineTest {
    @Test
    void bytePressureDrainsOwnedPagesBeforeReservingAnotherPage() {
        final int pageBytes = SenkuFlushPageTask.estimatedBytes(2, 10);
        final int heldBytes = SenkuFlushExecutor.PAGE_BYTES_BUDGET - pageBytes;
        SenkuFlushExecutor.reserve(heldBytes);
        try {
            assertTrue(writePipeline() > 0);
        } finally {
            SenkuFlushExecutor.release(heldBytes);
        }
        assertAllPermitsReturned();
    }

    @Test
    void concurrentPipelinesWaitForSharedBudgetAndBothFinish()
            throws InterruptedException, ExecutionException, TimeoutException {
        final int budget = SenkuFlushExecutor.PAGE_BYTES_BUDGET;
        SenkuFlushExecutor.reserve(budget);
        boolean budgetHeld = true;
        final CountDownLatch entered = new CountDownLatch(2);
        final FutureTask<Integer> first = new FutureTask<>(() -> {
            entered.countDown();
            return writePipeline();
        });
        final FutureTask<Integer> second = new FutureTask<>(() -> {
            entered.countDown();
            return writePipeline();
        });
        final Thread firstThread = new Thread(first, "senku-page-budget-first");
        final Thread secondThread = new Thread(second,
                "senku-page-budget-second");
        try {
            firstThread.start();
            secondThread.start();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            awaitWaiting(firstThread);
            awaitWaiting(secondThread);
            assertFalse(first.isDone());
            assertFalse(second.isDone());
            SenkuFlushExecutor.release(budget);
            budgetHeld = false;
            assertTrue(first.get(5, TimeUnit.SECONDS) > 0);
            assertTrue(second.get(5, TimeUnit.SECONDS) > 0);
        } finally {
            if (budgetHeld) {
                SenkuFlushExecutor.release(budget);
            }
            stopThread(firstThread);
            stopThread(secondThread);
        }
        assertAllPermitsReturned();
    }

    @Test
    void interruptedPipelineBudgetWaitPreservesInterruptAndReturnsAllBytes()
            throws InterruptedException {
        final int budget = SenkuFlushExecutor.PAGE_BYTES_BUDGET;
        SenkuFlushExecutor.reserve(budget);
        final CountDownLatch entered = new CountDownLatch(1);
        final AtomicBoolean interrupted = new AtomicBoolean();
        final FutureTask<Integer> task = new FutureTask<>(() -> {
            entered.countDown();
            try {
                return writePipeline();
            } finally {
                interrupted.set(Thread.currentThread().isInterrupted());
            }
        });
        final Thread thread = new Thread(task, "senku-page-budget-interrupt");
        try {
            thread.start();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            awaitWaiting(thread);
            thread.interrupt();
            final ExecutionException failure = assertThrows(
                    ExecutionException.class,
                    () -> task.get(5, TimeUnit.SECONDS));
            final IndexException storeFailure = assertInstanceOf(
                    IndexException.class, failure.getCause());
            assertInstanceOf(InterruptedException.class,
                    storeFailure.getCause());
            assertTrue(interrupted.get());
        } finally {
            SenkuFlushExecutor.release(budget);
            stopThread(thread);
        }
        assertAllPermitsReturned();
    }

    @Test
    void pagesAppendInShardOrderAcrossPartsAndEmptyShards() {
        final MemDirectory directory = new MemDirectory();
        final SenkuStorageFormat format = format();
        final LargeFileWriterTx writer = new LargeFileWriterTx(directory,
                DATA_BLOCK_SIZE, 3L, format);
        final long[] positions = pipeline(order(), 2).write(writer,
                new int[] { 0, 5, 5 }, new int[] { 5, 0, 5 });
        final int parts = writer.commit();
        assertTrue(parts > 1);
        assertEquals(0L, positions[0]);
        assertTrue(positions[2] > positions[0]);
        int count = 0;
        try (LargeFileReader reader = new LargeFile(directory, DATA_BLOCK_SIZE,
                3L, parts, format).openReader()) {
            ByteSequence page;
            while ((page = reader.read()) != null) {
                final LongKeyPageReader keys = new LongKeyPageReader(
                        format.keyCodec());
                try (MemFileReader input = new MemFileReader(
                        page.toByteArray())) {
                    Long key;
                    while ((key = keys.read(input)) != null) {
                        assertEquals(count++, key.longValue());
                    }
                }
            }
        }
        assertEquals(10, count);
        assertAllPermitsReturned();
    }

    @Test
    void failedEncodingJoinsOutstandingTasksAndReleasesAllBytes() {
        final MemDirectory directory = new MemDirectory();
        final LargeFileWriterTx writer = new LargeFileWriterTx(directory,
                DATA_BLOCK_SIZE, 10L, format());
        final SenkuFlushOrder<Long, NullValue> order = order();
        order.set(5, 4L, NULL);
        final SenkuFlushPagePipeline<Long, NullValue> pipeline = pipeline(order,
                2);
        final int[] starts = { 0 };
        final int[] counts = { 10 };
        final IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> pipeline.write(writer, starts, counts));
        writer.abort(failure);
        assertTrue(!directory.isFileExists(SenkuFileNames.MANIFEST_FILE));
        assertAllPermitsReturned();
    }

    @Test
    void appendFailureStillJoinsPreparedPagesWithoutPublishing() {
        final MemDirectory directory = new MemDirectory();
        directory.touch(SenkuFileNames.partFile(0));
        final LargeFileWriterTx writer = new LargeFileWriterTx(directory,
                DATA_BLOCK_SIZE, 10L, format());
        final SenkuFlushPagePipeline<Long, NullValue> pipeline = pipeline(
                order(), 2);
        final int[] starts = { 0 };
        final int[] counts = { 10 };
        final IndexException failure = assertThrows(IndexException.class,
                () -> pipeline.write(writer, starts, counts));
        writer.abort(failure);
        assertTrue(!directory.isFileExists(SenkuFileNames.MANIFEST_FILE));
        assertAllPermitsReturned();
    }

    private SenkuFlushPagePipeline<Long, NullValue> pipeline(
            final SenkuFlushOrder<Long, NullValue> order, final int pageSize) {
        return new SenkuFlushPagePipeline<>(order, new TypeDescriptorLong(),
                new TypeDescriptorNull(), format(), 10, pageSize);
    }

    private int writePipeline() {
        final LargeFileWriterTx writer = new LargeFileWriterTx(
                new MemDirectory(), DATA_BLOCK_SIZE, 3L, format());
        try {
            pipeline(order(), 2).write(writer, new int[] { 0 },
                    new int[] { 10 });
            return writer.commit();
        } catch (Exception failure) {
            writer.abort(failure);
            throw failure;
        }
    }

    private static void awaitWaiting(final Thread thread) {
        final long deadline = System.nanoTime()
                + Duration.ofSeconds(5).toNanos();
        while (thread.getState() != Thread.State.WAITING && thread.isAlive()
                && System.nanoTime() < deadline) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        assertEquals(Thread.State.WAITING, thread.getState(),
                "Pipeline must be waiting for its first page byte reservation");
    }

    private static void stopThread(final Thread thread)
            throws InterruptedException {
        thread.interrupt();
        thread.join(TimeUnit.SECONDS.toMillis(5));
        assertFalse(thread.isAlive(), "Pipeline caller did not terminate");
    }

    private SenkuStorageFormat format() {
        return new SenkuStorageFormat(KeyPageCodecs.longDeltaVarint(),
                Compression.zstd(3));
    }

    private SenkuFlushOrder<Long, NullValue> order() {
        final SenkuFlushOrder<Long, NullValue> order = new SenkuFlushOrder<>(
                new TypeDescriptorLong(), new TypeDescriptorNull(), 10);
        for (int index = 0; index < 10; index++) {
            order.set(index, (long) index, NULL);
        }
        return order;
    }

    private void assertAllPermitsReturned() {
        assertTrue(SenkuFlushExecutor
                .tryReserve(SenkuFlushExecutor.PAGE_BYTES_BUDGET));
        SenkuFlushExecutor.release(SenkuFlushExecutor.PAGE_BYTES_BUDGET);
    }
}
