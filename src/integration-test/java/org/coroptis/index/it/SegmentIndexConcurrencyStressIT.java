package org.coroptis.index.it;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
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
class SegmentIndexConcurrencyStressIT {

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
            "10,  500,   1200", //
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

    /**
     * Executes a single stress run: multiple worker threads do random
     * operations while a rotator closes and reopens the index a few times.
     */
    void runStressScenario(final String indexName, final long randomSeed,
            final int workerCount, final int opsPerWorker,
            final long timeoutInSeconds) throws Exception {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, Integer> conf = stressConf(indexName);
        final AtomicReference<SegmentIndex<Integer, Integer>> indexRef = new AtomicReference<>(
                SegmentIndex.create(directory, conf));
        final ReadWriteLock lifecycleLock = new ReentrantReadWriteLock();

        final int rotations = 3;
        final ExecutorService workers = Executors
                .newFixedThreadPool(workerCount);
        final ExecutorService rotator = Executors.newSingleThreadExecutor();
        final List<Future<?>> futures = new ArrayList<>();

        try {
            for (int t = 0; t < workerCount; t++) {
                final int workerId = t;
                futures.add(workers.submit(() -> {
                    final Random rnd = new Random(
                            1000L * workerId + randomSeed);
                    for (int i = 0; i < opsPerWorker; i++) {
                        final int op = rnd.nextInt(100);
                        lifecycleLock.readLock().lock();
                        try {
                            final SegmentIndex<Integer, Integer> index = indexRef
                                    .get();
                            final int key = rnd.nextInt(200);
                            if (op < 40) {
                                index.put(key, rnd.nextInt(10_000));
                            } else if (op < 65) {
                                index.get(key);
                            } else if (op < 80) {
                                index.delete(key);
                            } else if (op < 90) {
                                index.flush();
                            } else {
                                index.compact();
                            }
                        } finally {
                            lifecycleLock.readLock().unlock();
                        }
                        if ((i % 50) == 0) {
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
                        current.flush();
                        current.close();
                        indexRef.set(SegmentIndex.open(directory, conf));
                    } finally {
                        lifecycleLock.writeLock().unlock();
                    }
                }
                return null;
            }));

            for (final Future<?> future : futures) {
                future.get(timeoutInSeconds, TimeUnit.SECONDS);
            }
        } finally {
            workers.shutdownNow();
            rotator.shutdownNow();
            lifecycleLock.writeLock().lock();
            try {
                final SegmentIndex<Integer, Integer> current = indexRef.get();
                if (!current.wasClosed()) {
                    current.flushAndWait();
                    current.compactAndWait();
                    current.checkAndRepairConsistency();
                    current.close();
                }
            } finally {
                lifecycleLock.writeLock().unlock();
            }
        }
    }

    private static IndexConfiguration<Integer, Integer> stressConf(
            final String name) {
        return IndexConfiguration.<Integer, Integer>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(Integer.class)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorInteger())//
                .withName(name)//
                .withContextLoggingEnabled(false)//
                .withMaxNumberOfKeysInSegmentCache(30)//
                .withMaxNumberOfKeysInSegment(20)//
                .withMaxNumberOfKeysInSegmentChunk(5)//
                .withMaxNumberOfKeysInCache(60)//
                .withMaxNumberOfSegmentsInCache(10)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withIndexWorkerThreadCount(4)//
                .withNumberOfIoThreads(1)//
                .build();
    }
}
