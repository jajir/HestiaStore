package org.hestiastore.index.it;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Randomized concurrency stress test for index lifecycle.
 * <p>
 * Each repetition spins up worker threads that perform random
 * {@code put/get/delete/flush/compact} operations against a shared index, while
 * a single rotator thread periodically closes and reopens the same index
 * directory. A {@link ReadWriteLock} gates lifecycle transitions: operations
 * hold the read lock, rotations take the write lock. The test is deterministic
 * per repetition seed and fails on exceptions or timeouts.
 */
@Timeout(value = 300, unit = TimeUnit.SECONDS,
        threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class SegmentIndexConcurrencyStressIT {

    private static final long RETRY_TIMEOUT_MILLIS = 5_000L;
    private static final long RETRY_BACKOFF_MILLIS = 5L;
    private static final int TEST_CPU_THREADS = Math.max(1,
            Runtime.getRuntime().availableProcessors());
    private static final int MAX_TEST_WORKERS = Math.max(2,
            Math.min(8, TEST_CPU_THREADS * 2));

    /**
     * Repeats the stress run with a unique index name and seed to vary
     * interleavings while keeping runs deterministic.
     */
    @RepeatedTest(5)
    void test_concurrent_load(final RepetitionInfo repetitionInfo)
            throws Exception {
        runStressScenario(
                "stress-test-" + repetitionInfo.getCurrentRepetition(),
                23L + repetitionInfo.getCurrentRepetition(), 4, 400, 30);
    }

    @ParameterizedTest
    @CsvSource({ //
            "5,   400,     30", //
            "10,  500,    120", //
            "9, 3_000,    160" //
    })
    void test_concurrent_load_parametrized(final int workerCount,
            final int opsPerWorker, final long timeoutInSeconds)
            throws Exception {
        final int repetitionId = workerCount;

        runStressScenario(//
                "stress-test-" + repetitionId, //
                23L + repetitionId, workerCount, opsPerWorker,
                timeoutInSeconds);
    }

    @RepeatedTest(3)
    void test_concurrent_load_with_autonomous_split_and_rotations(
            final RepetitionInfo repetitionInfo) throws Exception {
        runAutonomousSplitStressScenario(
                "stress-autonomous-split-"
                        + repetitionInfo.getCurrentRepetition(),
                1_023L + repetitionInfo.getCurrentRepetition(), 6, 600, 60);
    }

    /**
     * Executes a single stress run: multiple worker threads do random
     * operations while a rotator closes and reopens the index a few times.
     */
    void runStressScenario(final String indexName, final long randomSeed,
            final int workerCount, final int opsPerWorker,
            final long timeoutInSeconds) throws Exception {
        final int effectiveWorkerCount = Math.max(2,
                Math.min(workerCount, MAX_TEST_WORKERS));
        final int effectiveOpsPerWorker = scaleOpsByCpu(opsPerWorker);
        final long effectiveTimeoutInSeconds = scaleTimeout(timeoutInSeconds);
        final int indexWorkerThreads = Math.min(4, TEST_CPU_THREADS);

        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, Integer> conf = stressConf(indexName,
                indexWorkerThreads);
        final AtomicReference<SegmentIndex<Integer, Integer>> indexRef = new AtomicReference<>(
                SegmentIndex.create(directory, conf));
        final ReadWriteLock lifecycleLock = new ReentrantReadWriteLock(true);

        final int rotations = TEST_CPU_THREADS <= 2 ? 2 : 3;
        final ExecutorService workers = Executors
                .newFixedThreadPool(effectiveWorkerCount);
        final ExecutorService rotator = Executors.newSingleThreadExecutor();
        final List<Future<?>> futures = new ArrayList<>();

        try {
            for (int t = 0; t < effectiveWorkerCount; t++) {
                final int workerId = t;
                futures.add(workers.submit(() -> {
                    final Random rnd = new Random(
                            1000L * workerId + randomSeed);
                    for (int i = 0; i < effectiveOpsPerWorker; i++) {
                        final int op = rnd.nextInt(100);
                        lifecycleLock.readLock().lock();
                        try {
                            final SegmentIndex<Integer, Integer> index = indexRef
                                    .get();
                            final int key = rnd.nextInt(200);
                            if (op < 45) {
                                index.put(key, rnd.nextInt(10_000));
                            } else if (op < 75) {
                                index.get(key);
                            } else if (op < 95) {
                                index.delete(key);
                            } else if (op < 99) {
                                index.flush();
                            } else {
                                index.compact();
                            }
                        } catch (final IndexException exception) {
                            if (!isTransientLifecycleFailure(exception)) {
                                throw exception;
                            }
                        } finally {
                            lifecycleLock.readLock().unlock();
                        }
                        if ((i % 100) == 0) {
                            Thread.yield();
                        }
                    }
                    return null;
                }));
            }

            futures.add(rotator.submit(() -> {
                final Random rnd = new Random(9000L + randomSeed);
                for (int i = 0; i < rotations; i++) {
                    try {
                        Thread.sleep(rnd.nextInt(20) + 5);
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return null;
                    }
                    lifecycleLock.writeLock().lock();
                    try {
                        final SegmentIndex<Integer, Integer> current = indexRef
                                .get();
                        current.flushAndWait();
                        current.close();
                        indexRef.set(openWithRetry(directory, conf));
                    } finally {
                        lifecycleLock.writeLock().unlock();
                    }
                }
                return null;
            }));

            for (final Future<?> future : futures) {
                future.get(effectiveTimeoutInSeconds, TimeUnit.SECONDS);
            }
        } finally {
            workers.shutdownNow();
            rotator.shutdownNow();
            lifecycleLock.writeLock().lock();
            try {
                final SegmentIndex<Integer, Integer> current = indexRef.get().orElse(null);
                if (!current.wasClosed()) {
                    current.flushAndWait();
                    current.compactAndWait();
                    checkAndRepairConsistencyWithRetry(current,
                            RETRY_TIMEOUT_MILLIS);
                    current.close();
                }
            } finally {
                lifecycleLock.writeLock().unlock();
            }
        }
    }

    void runAutonomousSplitStressScenario(final String indexName,
            final long randomSeed, final int workerCount,
            final int opsPerWorker, final long timeoutInSeconds)
            throws Exception {
        final int effectiveWorkerCount = Math.max(2,
                Math.min(workerCount, MAX_TEST_WORKERS));
        final int effectiveOpsPerWorker = scaleOpsByCpu(opsPerWorker);
        final long effectiveTimeoutInSeconds = scaleTimeout(timeoutInSeconds);
        final int indexWorkerThreads = Math.min(4, TEST_CPU_THREADS);
        final int hotKeyRange = 4_000;
        final int hotReadThreshold = 70;
        final int hotPutThreshold = 92;

        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, Integer> conf = autonomousSplitStressConf(
                indexName, indexWorkerThreads);
        final AtomicReference<SegmentIndex<Integer, Integer>> indexRef = new AtomicReference<>(
                SegmentIndex.create(directory, conf));
        final ReadWriteLock lifecycleLock = new ReentrantReadWriteLock(true);
        final AtomicBoolean splitObserved = new AtomicBoolean(false);

        lifecycleLock.writeLock().lock();
        try {
            final SegmentIndex<Integer, Integer> index = indexRef.get().orElse(null);
            for (int key = 0; key < hotKeyRange; key++) {
                index.put(key, -key);
            }
            index.flushAndWait();
            observeSplitEvidence(indexRef.get(), splitObserved);
        } finally {
            lifecycleLock.writeLock().unlock();
        }

        final int rotations = TEST_CPU_THREADS <= 2 ? 2 : 3;
        final ExecutorService workers = Executors
                .newFixedThreadPool(effectiveWorkerCount);
        final ExecutorService rotator = Executors.newSingleThreadExecutor();
        final List<Future<?>> futures = new ArrayList<>();

        try {
            for (int t = 0; t < effectiveWorkerCount; t++) {
                final int workerId = t;
                futures.add(workers.submit(() -> {
                    final Random rnd = new Random(
                            50_000L + 1000L * workerId + randomSeed);
                    for (int i = 0; i < effectiveOpsPerWorker; i++) {
                        final int op = rnd.nextInt(100);
                        lifecycleLock.readLock().lock();
                        try {
                            final SegmentIndex<Integer, Integer> index = indexRef
                                    .get();
                            final int key = selectAutonomousSplitKey(rnd,
                                    hotKeyRange, workerId);
                            if (op < hotReadThreshold) {
                                index.put(key, workerId * 1_000_000 + i);
                            } else if (op < hotPutThreshold) {
                                index.get(key);
                            } else if (op < 98) {
                                index.delete(key);
                            } else if (op < 99) {
                                index.flush();
                            } else {
                                index.compact();
                            }
                            observeSplitEvidence(index, splitObserved);
                        } catch (final IndexException exception) {
                            if (!isTransientLifecycleFailure(exception)) {
                                throw exception;
                            }
                        } finally {
                            lifecycleLock.readLock().unlock();
                        }
                        if ((i % 100) == 0) {
                            Thread.yield();
                        }
                    }
                    return null;
                }));
            }

            futures.add(rotator.submit(() -> {
                final Random rnd = new Random(99_000L + randomSeed);
                for (int i = 0; i < rotations; i++) {
                    try {
                        Thread.sleep(rnd.nextInt(25) + 10);
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return null;
                    }
                    lifecycleLock.writeLock().lock();
                    try {
                        final SegmentIndex<Integer, Integer> current = indexRef
                                .get();
                        current.flushAndWait();
                        observeSplitEvidence(current, splitObserved);
                        current.close();
                        indexRef.set(openWithRetry(directory, conf));
                        observeSplitEvidence(indexRef.get(), splitObserved);
                    } finally {
                        lifecycleLock.writeLock().unlock();
                    }
                }
                return null;
            }));

            for (final Future<?> future : futures) {
                future.get(effectiveTimeoutInSeconds, TimeUnit.SECONDS);
            }

            lifecycleLock.writeLock().lock();
            try {
                final SegmentIndex<Integer, Integer> current = indexRef.get().orElse(null);
                if (!current.wasClosed()) {
                    current.flushAndWait();
                    current.compactAndWait();
                    checkAndRepairConsistencyWithRetry(current,
                            RETRY_TIMEOUT_MILLIS);
                    observeSplitEvidence(current, splitObserved);
                }
            } finally {
                lifecycleLock.writeLock().unlock();
            }
            assertTrue(splitObserved.get(),
                    "Expected autonomous split evidence during lifecycle stress.");
        } finally {
            workers.shutdownNow();
            rotator.shutdownNow();
            lifecycleLock.writeLock().lock();
            try {
                final SegmentIndex<Integer, Integer> current = indexRef.get().orElse(null);
                if (!current.wasClosed()) {
                    current.close();
                }
            } finally {
                lifecycleLock.writeLock().unlock();
            }
        }
    }

    private static IndexConfiguration<Integer, Integer> stressConf(
            final String name, final int cpuThreads) {
        return IndexConfiguration.<Integer, Integer>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(Integer.class)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorInteger())//
                .withName(name)//
                .withContextLoggingEnabled(false)//
                .withBackgroundMaintenanceAutoEnabled(false)//
                .withMaxNumberOfKeysInActivePartition(256)//
                .withMaxNumberOfImmutableRunsPerPartition(4)//
                .withMaxNumberOfKeysInPartitionBuffer(1_024)//
                .withMaxNumberOfKeysInIndexBuffer(4_096)//
                .withMaxNumberOfKeysInPartitionBeforeSplit(10_000_000)//
                .withMaxNumberOfKeysInSegmentCache(30)//
                .withMaxNumberOfKeysInSegment(20)//
                .withMaxNumberOfKeysInSegmentChunk(5)//
                .withMaxNumberOfSegmentsInCache(10)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withNumberOfIndexMaintenanceThreads(
                        Math.max(1, Math.min(cpuThreads, 2)))//
                .withNumberOfRegistryLifecycleThreads(
                        Math.max(1, Math.min(cpuThreads, 2)))//
                .build();
    }

    private static IndexConfiguration<Integer, Integer> autonomousSplitStressConf(
            final String name, final int cpuThreads) {
        return IndexConfiguration.<Integer, Integer>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(Integer.class)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorInteger())//
                .withName(name)//
                .withContextLoggingEnabled(false)//
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
                .withNumberOfIndexMaintenanceThreads(
                        Math.max(1, Math.min(cpuThreads, 2)))//
                .withNumberOfRegistryLifecycleThreads(
                        Math.max(1, Math.min(cpuThreads, 2)))//
                .withIndexBusyTimeoutMillis(120_000)//
                .build();
    }

    private static int selectAutonomousSplitKey(final Random rnd,
            final int hotKeyRange, final int workerId) {
        if (rnd.nextInt(10) < 8) {
            return rnd.nextInt(hotKeyRange);
        }
        return 1_000_000 + workerId * 10_000 + rnd.nextInt(2_000);
    }

    private static void observeSplitEvidence(
            final SegmentIndex<Integer, Integer> index,
            final AtomicBoolean splitObserved) {
        if (splitObserved.get()) {
            return;
        }
        final var snapshot = index.metricsSnapshot();
        if (snapshot.getSplitScheduleCount() > 0
                || snapshot.getSplitInFlightCount() > 0
                || snapshot.getSegmentCount() > 1) {
            splitObserved.set(true);
        }
    }

    private static int scaleOpsByCpu(final int requestedOps) {
        if (TEST_CPU_THREADS >= 8) {
            return requestedOps;
        }
        if (TEST_CPU_THREADS >= 4) {
            return Math.max(requestedOps / 2, 200);
        }
        if (TEST_CPU_THREADS >= 2) {
            return Math.max(requestedOps / 3, 200);
        }
        return Math.max(requestedOps / 6, 120);
    }

    private static long scaleTimeout(final long requestedSeconds) {
        if (TEST_CPU_THREADS >= 4) {
            return requestedSeconds;
        }
        if (TEST_CPU_THREADS == 3) {
            return requestedSeconds + 15L;
        }
        if (TEST_CPU_THREADS == 2) {
            return requestedSeconds + 30L;
        }
        return requestedSeconds + 45L;
    }

    private static SegmentIndex<Integer, Integer> openWithRetry(
            final Directory directory,
            final IndexConfiguration<Integer, Integer> conf) {
        final long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(RETRY_TIMEOUT_MILLIS);
        while (true) {
            try {
                return SegmentIndex.open(directory, conf);
            } catch (final IndexException exception) {
                if (!isTransientLifecycleFailure(exception)
                        || System.nanoTime() >= deadline) {
                    throw exception;
                }
                sleepBriefly();
            }
        }
    }

    private static void checkAndRepairConsistencyWithRetry(
            final SegmentIndex<Integer, Integer> index,
            final long timeoutMillis) {
        final long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (true) {
            try {
                index.checkAndRepairConsistency();
                return;
            } catch (final RuntimeException exception) {
                if (!isTransientLifecycleFailure(exception)
                        || System.nanoTime() >= deadline) {
                    throw exception;
                }
                sleepBriefly();
            }
        }
    }

    private static boolean isTransientLifecycleFailure(
            final Throwable exception) {
        Throwable current = exception;
        while (current != null) {
            final String message = current.getMessage();
            if (message != null) {
                if (message.contains("There is no file 'manifest.txt'")
                        || message.contains("File manifest.txt does not exist")
                        || message.contains("timed out")
                        || message.contains("interrupted")
                        || message.contains("invalidated")) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }

    private static void sleepBriefly() {
        try {
            Thread.sleep(RETRY_BACKOFF_MILLIS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted while retrying transient lifecycle failure",
                    e);
        }
    }
}
