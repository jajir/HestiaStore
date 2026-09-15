package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.IndexException;
import org.junit.jupiter.api.Test;

class SenkuFlushExecutorTest {
    @Test
    void fourUnchangedMillionKeyPagesFitAndReleaseAllReservations() {
        final int pageBytes = SenkuFlushPageTask.estimatedBytes(1_000_000, 10);
        assertEquals(40_004_096, pageBytes);
        assertEquals(192 * 1024 * 1024, SenkuFlushExecutor.PAGE_BYTES_BUDGET);
        int reserved = 0;
        try {
            for (int page = 0; page < 4; page++) {
                assertTrue(SenkuFlushExecutor.tryReserve(pageBytes));
                reserved += pageBytes;
            }
            assertFalse(SenkuFlushExecutor.tryReserve(pageBytes * 2));
        } finally {
            SenkuFlushExecutor.release(reserved);
        }
        assertTrue(SenkuFlushExecutor
                .tryReserve(SenkuFlushExecutor.PAGE_BYTES_BUDGET));
        SenkuFlushExecutor.release(SenkuFlushExecutor.PAGE_BYTES_BUDGET);
    }

    @Test
    void failedSecondTaskWaitsForFirstToStopMutating()
            throws InterruptedException {
        assumeTrue(SenkuFlushExecutor.parallelism() > 1);
        final CountDownLatch firstEntered = new CountDownLatch(1);
        final CountDownLatch releaseFirst = new CountDownLatch(1);
        final CountDownLatch secondFailed = new CountDownLatch(1);
        final AtomicBoolean firstFinished = new AtomicBoolean();
        final AtomicReference<RuntimeException> failure = new AtomicReference<>();
        final IndexException expected = new IndexException("Second failed");
        final ForkJoinTask<?> first = ForkJoinTask.adapt(() -> {
            firstEntered.countDown();
            await(releaseFirst);
            firstFinished.set(true);
        });
        final ForkJoinTask<?> second = ForkJoinTask.adapt(() -> {
            await(firstEntered);
            secondFailed.countDown();
            throw expected;
        });
        final ForkJoinTask<?> parent = SenkuFlushExecutor
                .submit(ForkJoinTask.adapt(() -> {
                    try {
                        SenkuFlushExecutor.invokeSortPair(first, second);
                    } catch (RuntimeException exception) {
                        assertTrue(firstFinished.get());
                        failure.set(exception);
                    }
                }));
        try {
            assertTrue(secondFailed.await(5, TimeUnit.SECONDS));
            assertFalse(parent.isDone());
        } finally {
            releaseFirst.countDown();
            parent.join();
        }
        assertSame(expected, failure.get());
    }

    @Test
    void bothFailedTasksRetainPrimaryAndSuppressedFailure() {
        final IndexException firstFailure = new IndexException("First failed");
        final IndexException secondFailure = new IndexException(
                "Second failed");
        final AtomicReference<RuntimeException> failure = new AtomicReference<>();
        SenkuFlushExecutor.submit(ForkJoinTask.adapt(() -> {
            try {
                SenkuFlushExecutor.invokeSortPair(ForkJoinTask.adapt(() -> {
                    throw firstFailure;
                }), ForkJoinTask.adapt(() -> {
                    throw secondFailure;
                }));
            } catch (RuntimeException exception) {
                failure.set(exception);
            }
        })).join();
        assertSame(secondFailure, failure.get());
        assertEquals(1, failure.get().getSuppressed().length);
        final Throwable suppressed = failure.get().getSuppressed()[0];
        assertTrue(suppressed == firstFailure
                || suppressed.getCause() == firstFailure);
    }

    @Test
    void firstFailurePropagatesAfterSuccessfulSecondTask() {
        final AtomicBoolean secondFinished = new AtomicBoolean();
        final ForkJoinTask<?> task = SenkuFlushExecutor
                .submit(ForkJoinTask.adapt(() -> SenkuFlushExecutor
                        .invokeSortPair(ForkJoinTask.adapt(() -> {
                            throw new IndexException("First failed");
                        }), ForkJoinTask
                                .adapt(() -> secondFinished.set(true)))));
        assertThrows(IndexException.class, task::join);
        assertTrue(secondFinished.get());
    }

    @Test
    void pageByteBudgetIsSharedAndReusable() {
        final int budget = SenkuFlushExecutor.PAGE_BYTES_BUDGET;
        SenkuFlushExecutor.reserve(budget);
        try {
            assertFalse(SenkuFlushExecutor.tryReserve(1));
        } finally {
            SenkuFlushExecutor.release(budget);
        }
        assertTrue(SenkuFlushExecutor.tryReserve(budget));
        SenkuFlushExecutor.release(budget);
        assertTrue(SenkuFlushExecutor.parallelism() >= 1);
        assertTrue(SenkuFlushExecutor.parallelism() <= 4);
    }

    @Test
    void interruptionDoesNotConsumePermitsAndPreservesInterrupt() {
        Thread.currentThread().interrupt();
        try {
            assertThrows(IndexException.class,
                    () -> SenkuFlushExecutor.reserve(1));
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
        assertTrue(SenkuFlushExecutor
                .tryReserve(SenkuFlushExecutor.PAGE_BYTES_BUDGET));
        SenkuFlushExecutor.release(SenkuFlushExecutor.PAGE_BYTES_BUDGET);
    }

    private static void await(final CountDownLatch latch) {
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new AssertionError(exception);
        }
    }
}
