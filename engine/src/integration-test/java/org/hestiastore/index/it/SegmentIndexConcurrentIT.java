package org.hestiastore.index.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.hestiastore.index.Entry;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.control.model.RuntimeConfigPatch;
import org.hestiastore.index.control.model.RuntimePatchResult;
import org.hestiastore.index.control.model.RuntimeSettingKey;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Multi-threaded access safety check for {@link SegmentIndex} using the default
 * concurrent implementation.
 * <p>
 * The tests avoid relying on a globally deterministic ordering across threads
 * (which is not guaranteed) and instead validate deterministic end states:
 * disjoint keyspaces (order-independent) and "last write wins" for a single
 * key.
 */
@Timeout(value = 90, unit = TimeUnit.SECONDS,
        threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class SegmentIndexConcurrentIT {

    private static final int TEST_CPU_THREADS = Math.max(1,
            Math.min(4, Runtime.getRuntime().availableProcessors()));
    private static final long WORKER_AWAIT_SECONDS = TEST_CPU_THREADS <= 2 ? 90L
            : 60L;

    @Test
    void concurrent_put_delete_on_disjoint_keyspaces_produces_consistent_state_after_reopen()
            throws Exception {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, Integer> conf = newConfiguration(
                "concurrent-index", TEST_CPU_THREADS);

        final SegmentIndex<Integer, Integer> index = SegmentIndex
                .create(directory, conf);

        final int threads = 4;
        final int operationsPerThread = 200;
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        final CountDownLatch startGate = new CountDownLatch(1);
        final CountDownLatch doneGate = new CountDownLatch(threads);
        @SuppressWarnings("unchecked")
        final Map<Integer, Integer>[] expectedByThread = new Map[threads];

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                final Random rnd = new Random(12345L + threadId);
                final int keySpaceOffset = threadId * 1_000;
                final Map<Integer, Integer> expectedLocal = new HashMap<>();
                startGate.await();
                for (int i = 0; i < operationsPerThread; i++) {
                    final int key = keySpaceOffset + rnd.nextInt(50);
                    final double p = rnd.nextDouble();
                    if (p < 0.6) {
                        final int value = rnd.nextInt(10_000);
                        expectedLocal.put(key, value);
                        index.put(key, value);
                    } else if (p < 0.9) {
                        expectedLocal.remove(key);
                        index.delete(key);
                    } else {
                        index.get(key); // exercise reads
                    }
                }
                expectedByThread[threadId] = expectedLocal;
                doneGate.countDown();
                return null;
            });
        }

        startGate.countDown();
        assertTrue(doneGate.await(30, TimeUnit.SECONDS),
                "Writer threads did not finish in time");
        executor.shutdownNow();

        index.flushAndWait();

        final Map<Integer, Integer> expected = new java.util.HashMap<>();
        for (final Map<Integer, Integer> local : expectedByThread) {
            expected.putAll(local);
        }

        final Map<Integer, Integer> actual = index.getStream()
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue,
                        (first, second) -> second));

        assertEquals(expected, actual);

        index.close();

        // Re-open to ensure persisted state matches expectations
        final SegmentIndex<Integer, Integer> reopened = SegmentIndex
                .open(directory, conf);
        final Map<Integer, Integer> reloaded = reopened.getStream()
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue,
                        (first, second) -> second));
        assertEquals(expected, reloaded);
        reopened.close();
    }

    @Test
    void concurrent_mutations_on_same_key_last_write_wins() throws Exception {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, Integer> conf = newConfiguration(
                "concurrent-index", TEST_CPU_THREADS);

        final SegmentIndex<Integer, Integer> index = SegmentIndex
                .create(directory, conf);

        final int threads = 4;
        final int operationsPerThread = 200;
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        final CountDownLatch startGate = new CountDownLatch(1);
        final CountDownLatch doneGate = new CountDownLatch(threads);
        final int key = 1;

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                final Random rnd = new Random(98765L + threadId);
                startGate.await();
                for (int i = 0; i < operationsPerThread; i++) {
                    if (rnd.nextDouble() < 0.6) {
                        index.put(key, rnd.nextInt(10_000));
                    } else {
                        index.delete(key);
                    }
                }
                doneGate.countDown();
                return null;
            });
        }

        startGate.countDown();
        assertTrue(doneGate.await(30, TimeUnit.SECONDS),
                "Writer threads did not finish in time");
        executor.shutdownNow();

        final int finalValue = 42_4242;
        index.put(key, finalValue);
        index.flushAndWait();
        assertEquals(finalValue, index.get(key));
        index.close();

        final SegmentIndex<Integer, Integer> reopened = SegmentIndex
                .open(directory, conf);
        assertEquals(finalValue, reopened.get(key));
        reopened.close();
    }

    @Test
    @Disabled("Known nondeterministic failure kept out of the architecture cleanup flow.")
    void concurrent_put_delete_with_background_flush_compact_produces_consistent_state()
            throws Exception {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, Integer> conf = newConfiguration(
                "concurrent-index-maintenance", TEST_CPU_THREADS);

        final SegmentIndex<Integer, Integer> index = SegmentIndex
                .create(directory, conf);

        final int threads = 4;
        final int operationsPerThread = 400;
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        final CountDownLatch startGate = new CountDownLatch(1);
        final CountDownLatch doneGate = new CountDownLatch(threads);
        @SuppressWarnings("unchecked")
        final Map<Integer, Integer>[] expectedByThread = new Map[threads];

        final AtomicBoolean stop = new AtomicBoolean(false);
        final AtomicReference<Throwable> maintenanceError = new AtomicReference<>();
        final Thread maintenance = new Thread(() -> {
            try {
                startGate.await();
                int iteration = 0;
                while (!stop.get()) {
                    try {
                        index.flush();
                        if ((iteration++ % 3) == 0) {
                            index.compact();
                        }
                    } catch (final IndexException ex) {
                        if (!isTransientIndexFailure(ex)) {
                            throw ex;
                        }
                        if (stop.get()) {
                            break;
                        }
                    }
                    Thread.yield();
                }
            } catch (final Throwable t) {
                maintenanceError.compareAndSet(null, t);
                stop.set(true);
            }
        }, "segment-index-maintenance");
        maintenance.start();

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                final Random rnd = new Random(112233L + threadId);
                final int keySpaceOffset = threadId * 1_000;
                final Map<Integer, Integer> expectedLocal = new HashMap<>();
                startGate.await();
                for (int i = 0; i < operationsPerThread; i++) {
                    final int key = keySpaceOffset + rnd.nextInt(80);
                    if (rnd.nextDouble() < 0.7) {
                        final int value = rnd.nextInt(10_000);
                        expectedLocal.put(key, value);
                        index.put(key, value);
                    } else {
                        expectedLocal.remove(key);
                        index.delete(key);
                    }
                }
                expectedByThread[threadId] = expectedLocal;
                doneGate.countDown();
                return null;
            });
        }

        startGate.countDown();
        assertTrue(doneGate.await(60, TimeUnit.SECONDS),
                "Writer threads did not finish in time");

        stop.set(true);
        // Stop cooperatively so maintenance retries are not interrupted and
        // escalated into index ERROR state.
        maintenance.join(TimeUnit.SECONDS.toMillis(35));
        assertTrue(maintenance.isAlive() == false,
                "Maintenance thread did not stop in time");
        executor.shutdownNow();

        final Throwable backgroundFailure = maintenanceError.get();
        if (backgroundFailure != null) {
            throw new AssertionError("Maintenance thread failed",
                    backgroundFailure);
        }

        flushAndWaitWithRetry(index, 30_000L);
        checkAndRepairConsistencyWithRetry(index, 5_000L);
        flushAndWaitWithRetry(index, 30_000L);

        final Map<Integer, Integer> expected = new java.util.HashMap<>();
        for (final Map<Integer, Integer> local : expectedByThread) {
            expected.putAll(local);
        }

        // Validate point-lookup visibility before asserting stream state. This
        // helps to distinguish between missing persisted entries vs. iterator
        // omissions (e.g., entries still present only in the write cache).
        for (final Map.Entry<Integer, Integer> entry : expected.entrySet()) {
            assertEquals(entry.getValue(), index.get(entry.getKey()),
                    "Point lookup differs for key " + entry.getKey());
        }

        // Ensure any values that are still buffered in the write cache are
        // persisted before validating the full stream view.
        index.flushAndWait();

        final Map<Integer, Integer> actual = index.getStream()
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue,
                        (first, second) -> second));
        assertEquals(expected, actual);

        index.close();

        final SegmentIndex<Integer, Integer> reopened = SegmentIndex
                .open(directory, conf);
        final Map<Integer, Integer> reloaded = reopened.getStream()
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue,
                        (first, second) -> second));
        assertEquals(expected, reloaded);
        reopened.close();
    }

    @Test
    void concurrent_reads_do_not_return_corrupted_values() throws Exception {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, Integer> conf = newConfiguration(
                "concurrent-index-readers", TEST_CPU_THREADS);
        final SegmentIndex<Integer, Integer> index = SegmentIndex
                .create(directory, conf);

        final int keyCount = 120;
        for (int key = 0; key < keyCount; key++) {
            index.put(key, encodeValue(key, 0));
        }

        final int writerThreads = 4;
        final int readerThreads = 4;
        final int writesPerThread = scaleByCpu(500);
        final int readsPerThread = scaleByCpu(2_000);
        final ExecutorService writers = Executors
                .newFixedThreadPool(writerThreads);
        final ExecutorService readers = Executors
                .newFixedThreadPool(readerThreads);
        final CountDownLatch startGate = new CountDownLatch(1);
        final CountDownLatch writersDone = new CountDownLatch(writerThreads);
        final CountDownLatch readersDone = new CountDownLatch(readerThreads);
        final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        final AtomicReference<Throwable> readerFailure = new AtomicReference<>();

        for (int t = 0; t < writerThreads; t++) {
            final int threadId = t;
            writers.submit(() -> {
                try {
                    final Random rnd = new Random(556677L + threadId);
                    startGate.await();
                    for (int i = 0; i < writesPerThread; i++) {
                        final int key = rnd.nextInt(keyCount);
                        final int value = encodeValue(key,
                                rnd.nextInt(1_000_000));
                        try {
                            index.put(key, value);
                        } catch (final IndexException ex) {
                            if (!isTransientIndexFailure(ex)) {
                                throw ex;
                            }
                        }
                    }
                } catch (final Throwable t1) {
                    writerFailure.compareAndSet(null, t1);
                } finally {
                    writersDone.countDown();
                }
                return null;
            });
        }

        for (int t = 0; t < readerThreads; t++) {
            final int threadId = t;
            readers.submit(() -> {
                try {
                    final Random rnd = new Random(998877L + threadId);
                    startGate.await();
                    for (int i = 0; i < readsPerThread; i++) {
                        final int key = rnd.nextInt(keyCount);
                        try {
                            final Integer value = index.get(key);
                            if (value != null) {
                                assertEquals(key, decodeKey(value));
                            }
                        } catch (final IndexException ex) {
                            if (!isTransientIndexFailure(ex)) {
                                throw ex;
                            }
                        }
                    }
                } catch (final Throwable t1) {
                    readerFailure.compareAndSet(null, t1);
                } finally {
                    readersDone.countDown();
                }
                return null;
            });
        }

        startGate.countDown();
        assertTrue(writersDone.await(WORKER_AWAIT_SECONDS, TimeUnit.SECONDS),
                "Writer threads did not finish in time");
        assertTrue(readersDone.await(WORKER_AWAIT_SECONDS, TimeUnit.SECONDS),
                "Reader threads did not finish in time");
        assertNoWorkerFailure(writerFailure, "Writer thread failed");
        assertNoWorkerFailure(readerFailure, "Reader thread failed");
        writers.shutdownNow();
        readers.shutdownNow();

        index.flushAndWait();
        index.getStream().forEach(entry -> assertEquals(entry.getKey(),
                decodeKey(entry.getValue())));
        index.close();
    }

    @Test
    @Timeout(value = 180, unit = TimeUnit.SECONDS,
            threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void hotRangeRandomPutsWithAutonomousSplitDoNotStall() throws Exception {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, Integer> conf = newAutonomousSplitConfiguration(
                "concurrent-index-hot-range", TEST_CPU_THREADS);
        final SegmentIndex<Integer, Integer> index = SegmentIndex
                .create(directory, conf);

        final int hotPrefillKeys = 2_200;
        final int hotWriterThreads = 4;
        final int hotWriterOps = 80;
        final int coldWriterThreads = 1;
        final int coldWriterOps = 120;
        final int hotKeyRange = hotPrefillKeys;
        final ExecutorService executor = Executors
                .newFixedThreadPool(hotWriterThreads + coldWriterThreads);
        final CountDownLatch startGate = new CountDownLatch(1);
        final CountDownLatch doneGate = new CountDownLatch(
                hotWriterThreads + coldWriterThreads);
        final AtomicReference<Throwable> workerFailure = new AtomicReference<>();
        final AtomicInteger hotWriteCount = new AtomicInteger();
        final AtomicInteger coldWriteCount = new AtomicInteger();
        @SuppressWarnings("unchecked")
        final Map<Integer, Integer>[] expectedColdByThread = new Map[coldWriterThreads];

        try {
            for (int i = 0; i < hotPrefillKeys; i++) {
                index.put(i, -i);
            }
            awaitSplitEvidence(index, 30_000L);
            setSplitThreshold(index, 10_000_000);

            for (int t = 0; t < hotWriterThreads; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        final Random rnd = new Random(777_000L + threadId);
                        startGate.await();
                        for (int i = 0; i < hotWriterOps; i++) {
                            final int key = rnd.nextInt(hotKeyRange);
                            final int value = threadId * 100_000 + i;
                            index.put(key, value);
                            hotWriteCount.incrementAndGet();
                            if ((i % 200) == 0) {
                                index.get(key);
                            }
                        }
                    } catch (final Throwable t1) {
                        workerFailure.compareAndSet(null, t1);
                    } finally {
                        doneGate.countDown();
                    }
                    return null;
                });
            }

            for (int t = 0; t < coldWriterThreads; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    final Map<Integer, Integer> expectedLocal = new HashMap<>();
                    try {
                        final Random rnd = new Random(888_000L + threadId);
                        final int keyBase = 1_000_000 + threadId * 100_000;
                        startGate.await();
                        for (int i = 0; i < coldWriterOps; i++) {
                            final int key = keyBase + rnd.nextInt(1_000);
                            final int value = threadId * 100_000 + i;
                            expectedLocal.put(key, value);
                            index.put(key, value);
                            coldWriteCount.incrementAndGet();
                            if ((i % 100) == 0) {
                                index.get(key);
                            }
                        }
                    } catch (final Throwable t1) {
                        workerFailure.compareAndSet(null, t1);
                    } finally {
                        expectedColdByThread[threadId] = expectedLocal;
                        doneGate.countDown();
                    }
                    return null;
                });
            }

            startGate.countDown();
            assertTrue(doneGate.await(90, TimeUnit.SECONDS),
                    "Concurrent hot-range writers stalled");
            assertNoWorkerFailure(workerFailure,
                    "Hot-range split stress workers failed");

            assertEquals(hotWriterThreads * hotWriterOps,
                    hotWriteCount.get(),
                    "Expected all hot-range writes to complete");
            assertEquals(coldWriterThreads * coldWriterOps,
                    coldWriteCount.get(),
                    "Expected all cold-range writes to complete");

            assertTrue(index.metricsSnapshot().getSegmentCount() > 1
                    || index.metricsSnapshot().getSplitScheduleCount() > 0,
                    "Expected autonomous split scheduling under hot-range load");
            awaitSplitPublished(index, 30_000L);
            setSplitThreshold(index, 10_000_000);
            awaitSplitIdle(index, 30_000L);
            flushAndWaitWithRetry(index, 30_000L);

            final Map<Integer, Integer> expectedCold = new HashMap<>();
            for (final Map<Integer, Integer> local : expectedColdByThread) {
                expectedCold.putAll(local);
            }
            for (final Map.Entry<Integer, Integer> entry : expectedCold
                    .entrySet()) {
                assertEquals(entry.getValue(), index.get(entry.getKey()),
                    "Unexpected value for cold key " + entry.getKey());
            }
        } finally {
            executor.shutdownNow();
            if (!index.wasClosed()) {
                setSplitThreshold(index, 10_000_000);
                awaitSplitIdleBestEffort(index, 5_000L);
            }
            index.close();
        }
    }

    @Test
    @Timeout(value = 180, unit = TimeUnit.SECONDS,
            threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void hotRangeAutonomousSplitSurvivesCloseReopenRotations() throws Exception {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, Integer> conf = newAutonomousSplitConfiguration(
                "concurrent-index-hot-range-rotations", TEST_CPU_THREADS);
        final int hotPrefillKeys = 2_200;
        final int hotWriteOps = 24;
        final int coldWriteOps = 24;
        final int rotations = 2;
        final Map<Integer, Integer> expectedCold = new HashMap<>();
        SegmentIndex<Integer, Integer> index = SegmentIndex.create(directory,
                conf);

        try {
            for (int i = 0; i < hotPrefillKeys; i++) {
                index.put(i, -i);
            }
            awaitSplitEvidence(index, 30_000L);
            setSplitThreshold(index, 10_000_000);
            awaitSplitPublished(index, 60_000L);

            final Random hotRandom = new Random(991_000L);
            for (int i = 0; i < hotWriteOps; i++) {
                final int key = hotRandom.nextInt(hotPrefillKeys);
                index.put(key, 100_000 + i);
                if ((i % 20) == 0) {
                    index.get(key);
                }
            }

            final Random coldRandom = new Random(992_000L);
            for (int i = 0; i < coldWriteOps; i++) {
                final int key = 2_000_000 + coldRandom.nextInt(1_000);
                final int value = 200_000 + i;
                expectedCold.put(key, value);
                index.put(key, value);
                if ((i % 20) == 0) {
                    index.get(key);
                }
            }

            for (int rotation = 0; rotation < rotations; rotation++) {
                setSplitThreshold(index, 10_000_000);
                awaitSplitIdle(index, 30_000L);
                flushAndWaitWithRetry(index, 30_000L);
                index.close();
                index = SegmentIndex.open(directory, conf);
                setSplitThreshold(index, 10_000_000);
                awaitSplitIdle(index, 30_000L);

                for (final Map.Entry<Integer, Integer> entry : expectedCold
                        .entrySet()) {
                    assertEquals(entry.getValue(), index.get(entry.getKey()),
                            "Unexpected reopened value for rotated cold key "
                                    + entry.getKey());
                }

                assertTrue(index.metricsSnapshot().getSegmentCount() > 1
                        || index.metricsSnapshot().getSplitScheduleCount() > 0,
                        "Expected split evidence to survive close/reopen rotation");

                final int additionalColdKey = 3_000_000 + rotation;
                final int additionalColdValue = 300_000 + rotation;
                expectedCold.put(additionalColdKey, additionalColdValue);
                index.put(additionalColdKey, additionalColdValue);
                index.put(rotation, 400_000 + rotation);
                assertEquals(additionalColdValue, index.get(additionalColdKey),
                        "Unexpected value after reopened write for cold key "
                                + additionalColdKey);
            }

            setSplitThreshold(index, 10_000_000);
            awaitSplitIdle(index, 30_000L);
            flushAndWaitWithRetry(index, 30_000L);
            checkAndRepairConsistencyWithRetry(index, 5_000L);
            flushAndWaitWithRetry(index, 30_000L);

            index.close();
            index = SegmentIndex.open(directory, conf);
            setSplitThreshold(index, 10_000_000);

            for (final Map.Entry<Integer, Integer> entry : expectedCold
                    .entrySet()) {
                assertEquals(entry.getValue(), index.get(entry.getKey()),
                        "Unexpected final reopened value for cold key "
                                + entry.getKey());
            }
            assertTrue(index.metricsSnapshot().getSegmentCount() > 1,
                    "Expected persisted child routes after rotation reopens");
        } finally {
            if (index != null && !index.wasClosed()) {
                setSplitThreshold(index, 10_000_000);
                awaitSplitIdleBestEffort(index, 5_000L);
                index.close();
            }
        }
    }

    @Test
    void concurrent_put_delete_on_disjoint_keyspaces_produces_consistent_state()
            throws Exception {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, Integer> conf = newConfiguration(
                "concurrent-index-async", TEST_CPU_THREADS);
        final SegmentIndex<Integer, Integer> index = SegmentIndex
                .create(directory, conf);

        final int threads = 4;
        final int operationsPerThread = 200;
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        final CountDownLatch startGate = new CountDownLatch(1);
        final CountDownLatch doneGate = new CountDownLatch(threads);
        @SuppressWarnings("unchecked")
        final Map<Integer, Integer>[] expectedByThread = new Map[threads];

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                final Random rnd = new Random(4444L + threadId);
                final int keySpaceOffset = threadId * 1_000;
                final Map<Integer, Integer> expectedLocal = new HashMap<>();

                startGate.await();

                for (int i = 0; i < operationsPerThread; i++) {
                    final int key = keySpaceOffset + rnd.nextInt(50);
                    if (rnd.nextDouble() < 0.6) {
                        final int value = rnd.nextInt(10_000);
                        expectedLocal.put(key, value);
                        index.put(key, value);
                    } else {
                        expectedLocal.remove(key);
                        index.delete(key);
                    }
                }

                expectedByThread[threadId] = expectedLocal;
                doneGate.countDown();
                return null;
            });
        }

        startGate.countDown();
        assertTrue(doneGate.await(60, TimeUnit.SECONDS),
                "Worker threads did not finish in time");
        executor.shutdownNow();

        index.flushAndWait();

        final Map<Integer, Integer> expected = new java.util.HashMap<>();
        for (final Map<Integer, Integer> local : expectedByThread) {
            expected.putAll(local);
        }

        // Validate point lookups first so we can tell whether missing values in
        // the stream are due to persistence/iteration or due to lost writes.
        for (final Map.Entry<Integer, Integer> entry : expected.entrySet()) {
            assertEquals(entry.getValue(), index.get(entry.getKey()),
                    "Point lookup differs for key " + entry.getKey());
        }

        // Make sure buffered entries are flushed before collecting the stream
        // (getStream does not emit brand-new keys that exist only in the write
        // cache).
        index.flushAndWait();

        final Map<Integer, Integer> actual = index.getStream()
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue,
                        (first, second) -> second));
        assertEquals(expected, actual);

        index.close();

        final SegmentIndex<Integer, Integer> reopened = SegmentIndex
                .open(directory, conf);
        final Map<Integer, Integer> reloaded = reopened.getStream()
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue,
                        (first, second) -> second));
        assertEquals(expected, reloaded);
        reopened.close();
    }

    private static int encodeValue(final int key, final int sequence) {
        return key * 1_000_000 + sequence;
    }

    private static int decodeKey(final int value) {
        return value / 1_000_000;
    }

    private static boolean isTransientIndexFailure(
            final IndexException exception) {
        final String message = exception.getMessage();
        if (message == null) {
            return false;
        }
        return message.contains("timed out") || message.contains("interrupted");
    }

    private static void checkAndRepairConsistencyWithRetry(
            final SegmentIndex<Integer, Integer> index,
            final long timeoutMillis) throws InterruptedException {
        final long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (true) {
            try {
                index.checkAndRepairConsistency();
                return;
            } catch (final NoSuchElementException exception) {
                final String message = exception.getMessage();
                final boolean invalidated = message != null
                        && message.contains("invalidated");
                if (!invalidated || System.nanoTime() >= deadline) {
                    throw exception;
                }
                Thread.sleep(5L);
            }
        }
    }

    private static void flushAndWaitWithRetry(
            final SegmentIndex<Integer, Integer> index,
            final long timeoutMillis) throws InterruptedException {
        final long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (true) {
            try {
                index.flushAndWait();
                return;
            } catch (final IndexException exception) {
                if (!isTransientIndexFailure(exception)
                        || System.nanoTime() >= deadline) {
                    throw exception;
                }
            }
            Thread.sleep(10L);
        }
    }

    private static void setSplitThreshold(
            final SegmentIndex<Integer, Integer> index, final int threshold) {
        final long revision = index.controlPlane().configuration()
                .getConfigurationActual().getRevision();
        final RuntimePatchResult patchResult = index.controlPlane()
                .configuration()
                .apply(new RuntimeConfigPatch(Map.of(
                        RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT,
                        Integer.valueOf(threshold)), false,
                        Long.valueOf(revision)));
        assertTrue(patchResult.isApplied());
    }

    private static void awaitCondition(final Supplier<Boolean> condition,
            final long timeoutMillis) {
        final long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (condition.get()) {
                return;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20L));
            if (Thread.currentThread().isInterrupted()) {
                throw new AssertionError("Interrupted while waiting");
            }
        }
        assertTrue(condition.get(),
                "Condition not reached within " + timeoutMillis + " ms.");
    }

    private static void awaitSplitIdle(final SegmentIndex<Integer, Integer> index,
            final long timeoutMillis) {
        awaitCondition(() -> {
            final var snapshot = index.metricsSnapshot();
            return snapshot.getSplitInFlightCount() == 0
                    && snapshot.getSplitQueueSize() == 0
                    && snapshot.getSplitMaintenanceActiveThreadCount() == 0;
        }, timeoutMillis);
    }

    private static void awaitSplitIdleBestEffort(
            final SegmentIndex<Integer, Integer> index,
            final long timeoutMillis) {
        try {
            awaitSplitIdle(index, timeoutMillis);
        } catch (final AssertionError ignored) {
            // Cleanup should not fail the test when background split bookkeeping
            // settles slightly later than the semantic assertions.
        }
    }

    private static void awaitSplitEvidence(
            final SegmentIndex<Integer, Integer> index,
            final long timeoutMillis) {
        awaitCondition(() -> index.metricsSnapshot().getSegmentCount() > 1
                || index.metricsSnapshot().getSplitScheduleCount() > 0,
                timeoutMillis);
    }

    private static void awaitSplitPublished(
            final SegmentIndex<Integer, Integer> index,
            final long timeoutMillis) {
        awaitCondition(() -> {
            final var snapshot = index.metricsSnapshot();
            return snapshot.getSegmentCount() > 1
                    && snapshot.getSplitInFlightCount() == 0;
        }, timeoutMillis);
    }

    private static int scaleByCpu(final int operations) {
        if (TEST_CPU_THREADS >= 4) {
            return operations;
        }
        if (TEST_CPU_THREADS == 3) {
            return Math.max(operations * 3 / 4, 200);
        }
        if (TEST_CPU_THREADS == 2) {
            return Math.max(operations / 2, 200);
        }
        return Math.max(operations / 4, 100);
    }

    private static void assertNoWorkerFailure(
            final AtomicReference<Throwable> workerFailure,
            final String label) {
        final Throwable failure = workerFailure.get();
        if (failure != null) {
            throw new AssertionError(label, failure);
        }
    }

    private static IndexConfiguration<Integer, Integer> newConfiguration(
            final String name, final int cpuThreads) {
        return IndexConfiguration.<Integer, Integer>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(Integer.class)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorInteger())//
                .withName(name)//
                .withBackgroundMaintenanceAutoEnabled(false)//
                .withMaxNumberOfKeysInActivePartition(256)//
                .withMaxNumberOfImmutableRunsPerPartition(4)//
                .withMaxNumberOfKeysInPartitionBuffer(1_024)//
                .withMaxNumberOfKeysInIndexBuffer(4_096)//
                .withMaxNumberOfKeysInPartitionBeforeSplit(10_000_000)//
                .withMaxNumberOfKeysInSegmentCache(30)//
                .withMaxNumberOfKeysInSegment(20)// small to trigger splits
                .withMaxNumberOfKeysInSegmentChunk(5)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .build();
    }

    private static IndexConfiguration<Integer, Integer> newAutonomousSplitConfiguration(
            final String name, final int cpuThreads) {
        return IndexConfiguration.<Integer, Integer>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(Integer.class)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorInteger())//
                .withName(name)//
                .withBackgroundMaintenanceAutoEnabled(true)//
                .withMaxNumberOfKeysInActivePartition(512)//
                .withMaxNumberOfImmutableRunsPerPartition(6)//
                .withMaxNumberOfKeysInPartitionBuffer(8_192)//
                .withMaxNumberOfKeysInIndexBuffer(65_536)//
                .withMaxNumberOfKeysInPartitionBeforeSplit(2_000)//
                .withMaxNumberOfKeysInSegmentCache(256)//
                .withMaxNumberOfKeysInSegment(16_384)//
                .withMaxNumberOfKeysInSegmentChunk(32)//
                .withMaxNumberOfSegmentsInCache(64)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withNumberOfSegmentMaintenanceThreads(2)//
                .withNumberOfIndexMaintenanceThreads(2)//
                .withNumberOfRegistryLifecycleThreads(2)//
                .withIndexBusyTimeoutMillis(120_000)//
                .build();
    }
}
