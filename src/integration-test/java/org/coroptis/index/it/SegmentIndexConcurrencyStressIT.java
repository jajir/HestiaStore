package org.coroptis.index.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hestiastore.index.datatype.ConvertorFromBytes;
import org.hestiastore.index.datatype.ConvertorToBytes;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorString;
import org.hestiastore.index.datatype.TypeReader;
import org.hestiastore.index.datatype.TypeWriter;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
import org.junit.jupiter.api.Test;

class SegmentIndexConcurrencyStressIT {

    @RepeatedTest(3)
    void randomizedConcurrentOperationsWithRotation(
            final RepetitionInfo repetitionInfo) throws Exception {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, Integer> conf = stressConf(
                "stress-index-" + repetitionInfo.getCurrentRepetition());
        final AtomicReference<SegmentIndex<Integer, Integer>> indexRef = new AtomicReference<>(
                SegmentIndex.create(directory, conf));
        final ReadWriteLock lifecycleLock = new ReentrantReadWriteLock();

        final int workerCount = 4;
        final int opsPerWorker = 400;
        final int rotations = 3;
        final ExecutorService workers = Executors
                .newFixedThreadPool(workerCount);
        final ExecutorService rotator = Executors.newSingleThreadExecutor();
        final List<Future<?>> futures = new ArrayList<>();

        try {
            for (int t = 0; t < workerCount; t++) {
                final int workerId = t;
                futures.add(workers.submit(() -> {
                    final Random rnd = new Random(1000L * workerId
                            + repetitionInfo.getCurrentRepetition());
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
                final Random rnd = new Random(
                        9000L + repetitionInfo.getCurrentRepetition());
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
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            workers.shutdownNow();
            rotator.shutdownNow();
            lifecycleLock.writeLock().lock();
            try {
                final SegmentIndex<Integer, Integer> current = indexRef.get();
                if (!current.wasClosed()) {
                    current.checkAndRepairConsistency();
                    current.close();
                }
            } finally {
                lifecycleLock.writeLock().unlock();
            }
        }
    }

    /**
     * 
     * Test verify that gets really overlaps.
     * 
     * 1) Test put new key value pait into index
     * 
     * 2) Test start two get(k) in parallel threads. Execution of both gets
     * should overlap
     * 
     * 
     * @throws Exception
     */
    @Test
    void parallelReadsOverlap_with_two_threads() throws Exception {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<String, String> conf = readLockConf(
                "read-lock-overlap", 2);
        final SegmentIndex<String, String> index = SegmentIndex
                .create(directory, conf);
        index.put("k", "v");
        final BlockingTombstoneTypeDescriptorString.Hook hook = BlockingTombstoneTypeDescriptorString
                .installHook();
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            final Future<String> first = executor.submit(() -> index.get("k"));
            assertTrue(hook.firstEntered.await(5, TimeUnit.SECONDS),
                    "First get() is waiting for tombstone hook");

            final Future<String> second = executor.submit(() -> index.get("k"));
            assertTrue(hook.secondEntered.await(5, TimeUnit.SECONDS),
                    "Second get() is waiting for tombstone hook");

            assertTrue(hook.overlapDetected.get(),
                    "Expected overlapping reads with two threads");
            assertFalse(first.isDone(),
                    "First get() returned before being released");
            assertFalse(second.isDone(),
                    "Second get() returned before being released");
            // release both get() operation waitings
            hook.release.countDown();

            assertEquals("v", first.get(5, TimeUnit.SECONDS));
            assertEquals("v", second.get(5, TimeUnit.SECONDS));
            assertTrue(first.isDone(),
                    "First get() is still not done after being released");
            assertTrue(second.isDone(),
                    "Second get() is still not done after being released");
        } finally {
            hook.release.countDown();
            BlockingTombstoneTypeDescriptorString.clearHook();
            executor.shutdownNow();
            index.close();
        }
    }

    @Test
    void parallelReads_areSerialized_with_one_threads() throws Exception {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<String, String> conf = readLockConf(
                "read-lock-overlap", 1);
        final SegmentIndex<String, String> index = SegmentIndex
                .create(directory, conf);
        index.put("k", "v");
        final BlockingTombstoneTypeDescriptorString.Hook hook = BlockingTombstoneTypeDescriptorString
                .installHook();
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            final Future<String> first = executor.submit(() -> index.get("k"));
            assertTrue(hook.firstEntered.await(5, TimeUnit.SECONDS),
                    "First get() is waiting for tombstone hook");

            final Future<String> second = executor.submit(() -> index.get("k"));
            assertFalse(hook.secondEntered.await(200, TimeUnit.MILLISECONDS),
                    "Second get() entered before first was released");
            assertFalse(hook.overlapDetected.get(),
                    "Detected overlapping reads with one thread");

            assertFalse(first.isDone(),
                    "First get() returned before being released");
            assertFalse(second.isDone(),
                    "Second get() returned before being scheduled");
            // release both get() operation waitings
            hook.release.countDown();

            assertTrue(hook.secondEntered.await(5, TimeUnit.SECONDS),
                    "Second get() did not execute after release");
            assertEquals("v", first.get(5, TimeUnit.SECONDS));
            assertEquals("v", second.get(5, TimeUnit.SECONDS));
            assertFalse(hook.overlapDetected.get(),
                    "Detected overlapping reads with one thread");
            assertTrue(first.isDone(),
                    "First get() is still not done after being released");
            assertTrue(second.isDone(),
                    "Second get() is still not done after being released");
        } finally {
            hook.release.countDown();
            BlockingTombstoneTypeDescriptorString.clearHook();
            executor.shutdownNow();
            index.close();
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
                .withMaxNumberOfKeysInSegment(20)//
                .withMaxNumberOfKeysInSegmentCache(30)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(40)//
                .withMaxNumberOfKeysInSegmentChunk(5)//
                .withMaxNumberOfKeysInCache(60)//
                .withMaxNumberOfSegmentsInCache(10)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withNumberOfCpuThreads(4)//
                .withNumberOfIoThreads(1)//
                .build();
    }

    private static IndexConfiguration<String, String> readLockConf(
            final String name, int numberOfCpuThreads) {
        return IndexConfiguration.<String, String>builder()//
                .withKeyClass(String.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(new TypeDescriptorString())//
                .withValueTypeDescriptor(
                        new BlockingTombstoneTypeDescriptorString())//
                .withName(name)//
                .withContextLoggingEnabled(false)//
                .withMaxNumberOfKeysInCache(10_000)//
                .withNumberOfCpuThreads(numberOfCpuThreads)//
                .withNumberOfIoThreads(1)//
                .build();
    }

    public static final class BlockingTombstoneTypeDescriptorString
            implements TypeDescriptor<String> {

        static final class Hook {
            final CountDownLatch firstEntered = new CountDownLatch(1);
            final CountDownLatch secondEntered = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);

            /**
             * Number of calls currently in getTombstone()
             */
            final AtomicInteger inCall = new AtomicInteger(0);

            final AtomicInteger totalEntered = new AtomicInteger(0);
            /**
             * Set to true when overlapping calls are detected
             */
            final AtomicBoolean overlapDetected = new AtomicBoolean(false);
        }

        private static final AtomicReference<Hook> HOOK = new AtomicReference<>();

        static Hook installHook() {
            final Hook hook = new Hook();
            HOOK.set(hook);
            return hook;
        }

        static void clearHook() {
            HOOK.set(null);
        }

        private final TypeDescriptorString delegate = new TypeDescriptorString();

        @Override
        public java.util.Comparator<String> getComparator() {
            return delegate.getComparator();
        }

        @Override
        public TypeReader<String> getTypeReader() {
            return delegate.getTypeReader();
        }

        @Override
        public TypeWriter<String> getTypeWriter() {
            return delegate.getTypeWriter();
        }

        @Override
        public ConvertorFromBytes<String> getConvertorFromBytes() {
            return delegate.getConvertorFromBytes();
        }

        @Override
        public ConvertorToBytes<String> getConvertorToBytes() {
            return delegate.getConvertorToBytes();
        }

        @Override
        public String getTombstone() {
            final Hook hook = HOOK.get();
            if (hook != null) {
                final int concurrent = hook.inCall.incrementAndGet();
                if (concurrent > 1) {
                    hook.overlapDetected.set(true);
                }
                final int total = hook.totalEntered.incrementAndGet();
                if (total == 1) {
                    hook.firstEntered.countDown();
                } else if (total == 2) {
                    hook.secondEntered.countDown();
                }
                try {
                    if (!hook.release.await(6, TimeUnit.SECONDS)) {
                        throw new TimeoutException(
                                "Timed out waiting for read overlap hook.");
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(
                            "Interrupted while waiting on read overlap hook",
                            e);
                } catch (final TimeoutException e) {
                    throw new IllegalStateException(e.getMessage(), e);
                } finally {
                    hook.inCall.decrementAndGet();
                }
            }
            return delegate.getTombstone();
        }
    }
}
