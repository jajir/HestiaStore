package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorNull;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.senku.SenkuMergeFunctions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests real batch condition waits without index locks or timing sleeps. */
class SenkuLongBatchAdmissionIT {
    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void pausedBatchesResumeOrInterruptWithoutHoldingMutationLocks(
            final boolean interrupt) throws InterruptedException {
        final CountDownLatch hashed = new CountDownLatch(1);
        final CountDownLatch finished = new CountDownLatch(1);
        final AtomicReference<Exception> failure = new AtomicReference<>();
        final AtomicBoolean interrupted = new AtomicBoolean();
        final SenkuFlushWriter<Long, NullValue> writer = new SenkuFlushWriter<>(
                new MemDirectory(), new TypeDescriptorLong(),
                new TypeDescriptorNull(), key -> key.hashCode(), 1, 10, 100L,
                DataBlockSize.ofDataBlockSize(8192));
        final SenkuIngestor<Long, NullValue> ingestor = new SenkuIngestor<>(
                new ReentrantLock(), SenkuMergeFunctions.longSet(),
                key -> key.hashCode(), writer, 100, 4, key -> {
                    hashed.countDown();
                    return 0;
                });
        ingestor.setPaused(true);
        final long[] input = { 7L };
        ingestor.putLongs(input, input.length, 0);
        final Thread caller = new Thread(() -> {
            try {
                ingestor.putLongs(input, 0, 1);
            } catch (Exception e) {
                failure.set(e);
                interrupted.set(Thread.currentThread().isInterrupted());
            } finally {
                finished.countDown();
            }
        }, "controlled-primitive-batch");
        caller.start();
        try {
            assertTrue(hashed.await(10, TimeUnit.SECONDS));
            awaitWaiting(caller);
            assertEquals(0, ingestor.size(),
                    "An admission waiter owns no mutation stripe");
            if (interrupt) {
                caller.interrupt();
            } else {
                ingestor.setPaused(false);
            }
            assertTrue(finished.await(10, TimeUnit.SECONDS));
            if (interrupt) {
                assertInstanceOf(IndexException.class, failure.get());
                assertInstanceOf(InterruptedException.class,
                        failure.get().getCause());
                assertTrue(interrupted.get());
                assertEquals(0, ingestor.size());
            } else {
                assertNull(failure.get());
                assertEquals(1, ingestor.size());
            }
        } finally {
            ingestor.fail();
            caller.interrupt();
            caller.join(TimeUnit.SECONDS.toMillis(10));
            assertFalse(caller.isAlive());
        }
    }

    /** Waits for the controlled caller's untimed admission condition. */
    private static void awaitWaiting(final Thread caller) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (caller.getState() != Thread.State.WAITING
                && System.nanoTime() < deadline) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        assertEquals(Thread.State.WAITING, caller.getState());
    }
}
