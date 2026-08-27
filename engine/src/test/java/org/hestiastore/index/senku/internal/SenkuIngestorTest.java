package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.senku.internal.LargeFileTestSupport.DATA_BLOCK_SIZE;
import static org.hestiastore.index.senku.internal.SenkuFlushTestSupport.readShard;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Comparator;
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
    void concurrentPutsDoNotLoseMerges()
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
    void concurrentUniquePutsSurviveRepeatedRotations()
            throws InterruptedException {
        final int threadCount = 4;
        final int keysPerThread = 64;
        final SenkuIngestor<Integer, Long> ingestor = newIngestor(16,
                (key, first, second) -> first + second);
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threadCount);
        final ExecutorService executor = Executors
                .newFixedThreadPool(threadCount);
        try {
            for (int thread = 0; thread < threadCount; thread++) {
                final int firstKey = thread * keysPerThread;
                executor.execute(() -> {
                    try {
                        start.await();
                        for (int offset = 0; offset < keysPerThread; offset++) {
                            ingestor.put(firstKey + offset, 1L);
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

        ingestor.stopAcceptingAndFlush();
        final int generationCount = names().size();
        assertTrue(generationCount > 1);
        final List<Entry<Integer, Long>> actual = new ArrayList<>();
        for (long generation = 0; generation < generationCount; generation++) {
            actual.addAll(readShard(flushDirectory, generation, 0, 1, 10L));
        }
        actual.sort(Comparator.comparing(Entry::getKey));
        final List<Entry<Integer, Long>> expected = new ArrayList<>();
        for (int key = 0; key < threadCount * keysPerThread; key++) {
            expected.add(Entry.of(key, 1L));
        }
        assertEquals(expected, actual);
    }

    @Test
    void distinctKeyMergesRunConcurrently() throws Exception {
        final CountDownLatch mergesEntered = new CountDownLatch(2);
        final CountDownLatch releaseMerges = new CountDownLatch(1);
        final SenkuIngestor<Integer, Long> ingestor = newIngestor(10,
                (key, first, second) -> {
                    mergesEntered.countDown();
                    try {
                        if (!releaseMerges.await(5, TimeUnit.SECONDS)) {
                            throw new IllegalStateException(
                                    "Timed out waiting for concurrent merge.");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(
                                "Concurrent merge was interrupted.", e);
                    }
                    return first + second;
                });
        ingestor.put(1, 1L);
        ingestor.put(2, 1L);
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            final Future<?> first = executor.submit(() -> ingestor.put(1, 1L));
            final Future<?> second = executor.submit(() -> ingestor.put(2, 1L));

            assertTrue(mergesEntered.await(1, TimeUnit.SECONDS));
            releaseMerges.countDown();
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
        } finally {
            releaseMerges.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }

        assertEquals(2, ingestor.size());
        assertTrue(ingestor.flushRemaining());
        assertEquals(List.of(Entry.of(1, 2L), Entry.of(2, 2L)),
                readShard(flushDirectory, 0L, 0, 1, 10L));
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

    @Test
    void stripeMixerDistributesHashesWithFixedHashMapBucketBits() {
        final int[] counts = new int[32];
        for (int value = 0; value < 32_768; value++) {
            final int upperBits = value << 5;
            final int hash = upperBits | (upperBits >>> 16 & 31);
            assertEquals(0, (hash ^ hash >>> 16) & 31);
            counts[SenkuIngestor.stripeFromHash(hash)]++;
        }

        int minimum = Integer.MAX_VALUE;
        int maximum = Integer.MIN_VALUE;
        for (int count : counts) {
            minimum = Math.min(minimum, count);
            maximum = Math.max(maximum, count);
        }
        assertTrue(minimum > 0);
        assertTrue(maximum - minimum < 256,
                "independent stripe mixer should remain balanced");
    }

    @Test
    void stripeMixerHasStableResultsForExtremeHashes() {
        assertEquals(4, SenkuIngestor.stripeFromHash(Integer.MIN_VALUE));
        assertEquals(24, SenkuIngestor.stripeFromHash(Integer.MAX_VALUE));
    }

    private SenkuIngestor<Integer, Long> newIngestor(
            final int maxInMemoryEntries,
            final SenkuMergeFunction<Integer, Long> mergeFunction) {
        final SenkuFlushWriter<Integer, Long> flushWriter =
                new SenkuFlushWriter<>(flushDirectory,
                        new TypeDescriptorInteger(), new TypeDescriptorLong(),
                        value -> 0, 1, 10, 10L, DATA_BLOCK_SIZE);
        return new SenkuIngestor<>(new ReentrantLock(), mergeFunction,
                value -> value, flushWriter, maxInMemoryEntries,
                maxInMemoryEntries * 2);
    }

    private Set<String> names() {
        return flushDirectory.getFileNames().collect(Collectors.toSet());
    }
}
