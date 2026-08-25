package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.senku.internal.LargeFileTestSupport.DATA_BLOCK_SIZE;
import static org.hestiastore.index.senku.internal.SenkuFlushTestSupport.readShard;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.hestiastore.index.Entry;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.senku.SenkuMergeFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SenkuIngestorTest {

    private MemDirectory flushDirectory;

    @BeforeEach
    void setUp() {
        flushDirectory = new MemDirectory();
    }

    @Test
    void duplicatePutsMergeWithoutIncreasingFlushThreshold() {
        final SenkuIngestor<Integer, Long> ingestor = newIngestor(2,
                (key, first, second) -> first + second);

        ingestor.put(1, 2L);
        ingestor.put(1, 3L);
        assertEquals(1, ingestor.size());
        assertEquals(Set.of(), names());

        ingestor.put(2, 4L);
        assertEquals(0, ingestor.size());
        assertEquals(List.of(Entry.of(1, 5L), Entry.of(2, 4L)),
                readShard(flushDirectory, 0L, 0, 1, 10L));
    }

    @Test
    void exactThresholdCreatesMonotonicFlushGenerations() {
        final SenkuIngestor<Integer, Long> ingestor = newIngestor(1,
                (key, first, second) -> first + second);

        ingestor.put(1, 1L);
        ingestor.put(2, 2L);

        assertEquals(Set.of("flush-00000", "flush-00001"), names());
    }

    @Test
    void flushRemainingPublishesOnlyNonEmptyMap() {
        final SenkuIngestor<Integer, Long> ingestor = newIngestor(10,
                (key, first, second) -> first + second);

        assertFalse(ingestor.flushRemaining());
        ingestor.put(1, 1L);
        assertTrue(ingestor.flushRemaining());
        assertFalse(ingestor.flushRemaining());
        assertEquals(Set.of("flush-00000"), names());
    }

    @Test
    void putRejectsNullWithoutChangingMap() {
        final SenkuIngestor<Integer, Long> ingestor = newIngestor(10,
                (key, first, second) -> first + second);

        assertThrows(IllegalArgumentException.class,
                () -> ingestor.put(null, 1L));
        assertThrows(IllegalArgumentException.class,
                () -> ingestor.put(1, null));
        assertEquals(0, ingestor.size());
    }

    @Test
    void mergeFailureDoesNotReplaceExistingValue() {
        final IllegalStateException cause = new IllegalStateException("boom");
        final SenkuIngestor<Integer, Long> ingestor = newIngestor(10,
                (key, first, second) -> {
                    throw cause;
                });
        ingestor.put(1, 1L);

        final IndexException error = assertThrows(IndexException.class,
                () -> ingestor.put(1, 2L));

        assertEquals(cause, error.getCause());
        assertEquals(1, ingestor.size());
    }

    @Test
    void nullMergeResultFailsWithoutReplacingExistingValue() {
        final SenkuIngestor<Integer, Long> ingestor = newIngestor(10,
                (key, first, second) -> null);
        ingestor.put(1, 1L);

        assertThrows(IndexException.class, () -> ingestor.put(1, 2L));
        assertEquals(1, ingestor.size());
    }

    @Test
    void concurrentPutsAreSerializedWithoutLostMerges()
            throws InterruptedException {
        final int threadCount = 4;
        final int keyCount = 100;
        final SenkuIngestor<Integer, Long> ingestor = newIngestor(1_000,
                (key, first, second) -> first + second);
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threadCount);
        final ExecutorService executor = Executors
                .newFixedThreadPool(threadCount);
        try {
            for (int thread = 0; thread < threadCount; thread++) {
                executor.execute(() -> {
                    try {
                        start.await();
                        for (int key = 0; key < keyCount; key++) {
                            ingestor.put(key, 1L);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(10, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }

        assertEquals(keyCount, ingestor.size());
        assertTrue(ingestor.flushRemaining());
        final List<Entry<Integer, Long>> actual = readShard(flushDirectory, 0L,
                0, 1, 10L);
        final List<Entry<Integer, Long>> expected = new ArrayList<>();
        for (int key = 0; key < keyCount; key++) {
            expected.add(Entry.of(key, (long) threadCount));
        }
        assertEquals(expected, actual);
    }

    @Test
    void pausedPutBlocksUntilLowWaterResume() throws Exception {
        final SenkuIngestor<Integer, Long> ingestor = newIngestor(10,
                (key, first, second) -> first + second);
        ingestor.setPaused(true);
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            final Future<?> put = executor.submit(() -> ingestor.put(1, 1L));

            assertThrows(TimeoutException.class,
                    () -> put.get(50, TimeUnit.MILLISECONDS));
            assertTrue(ingestor.isPaused());
            ingestor.setPaused(false);
            put.get(5, TimeUnit.SECONDS);

            assertFalse(ingestor.isPaused());
            assertEquals(1, ingestor.size());
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void stopAcceptingWakesBlockedPutAndFlushesExistingEntries()
            throws Exception {
        final SenkuIngestor<Integer, Long> ingestor = newIngestor(10,
                (key, first, second) -> first + second);
        ingestor.put(1, 1L);
        ingestor.setPaused(true);
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            final Future<?> blocked = executor.submit(() -> ingestor.put(2, 2L));
            assertThrows(TimeoutException.class,
                    () -> blocked.get(50, TimeUnit.MILLISECONDS));

            assertTrue(ingestor.stopAcceptingAndFlush());
            final ExecutionException failure = assertThrows(
                    ExecutionException.class, blocked::get);

            assertTrue(failure.getCause() instanceof IndexException);
            assertEquals(List.of(Entry.of(1, 1L)),
                    readShard(flushDirectory, 0L, 0, 1, 10L));
            assertThrows(IndexException.class, () -> ingestor.put(3, 3L));
            assertThrows(IndexException.class,
                    ingestor::stopAcceptingAndFlush);
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void interruptedPausedCallerKeepsInterruptStatusAfterResume()
            throws InterruptedException {
        final SenkuIngestor<Integer, Long> ingestor = newIngestor(10,
                (key, first, second) -> first + second);
        ingestor.setPaused(true);
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(1);
        final AtomicBoolean interrupted = new AtomicBoolean();
        final Thread caller = new Thread(() -> {
            started.countDown();
            ingestor.put(1, 1L);
            interrupted.set(Thread.currentThread().isInterrupted());
            done.countDown();
        });
        caller.start();
        assertTrue(started.await(5, TimeUnit.SECONDS));

        caller.interrupt();
        ingestor.setPaused(false);

        assertTrue(done.await(5, TimeUnit.SECONDS));
        assertTrue(interrupted.get());
    }

    @Test
    void failClearsPauseAndRejectsFurtherWrites() {
        final SenkuIngestor<Integer, Long> ingestor = newIngestor(10,
                (key, first, second) -> first + second);
        ingestor.setPaused(true);

        ingestor.fail();

        assertFalse(ingestor.isPaused());
        assertThrows(IndexException.class, () -> ingestor.put(1, 1L));
    }

    private SenkuIngestor<Integer, Long> newIngestor(
            final int maxInMemoryEntries,
            final SenkuMergeFunction<Integer, Long> mergeFunction) {
        final SenkuFlushWriter<Integer, Long> flushWriter =
                new SenkuFlushWriter<>(flushDirectory,
                        new TypeDescriptorInteger(), new TypeDescriptorLong(),
                        value -> 0, 1, 10, 10L, DATA_BLOCK_SIZE);
        return new SenkuIngestor<>(new ReentrantLock(), mergeFunction,
                flushWriter, maxInMemoryEntries, maxInMemoryEntries * 2);
    }

    private Set<String> names() {
        return flushDirectory.getFileNames().collect(Collectors.toSet());
    }
}
