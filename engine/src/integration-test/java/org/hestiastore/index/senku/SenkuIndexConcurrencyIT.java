package org.hestiastore.index.senku;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class SenkuIndexConcurrencyIT {

    @Test
    void concurrentDuplicateAndDisjointPutsHaveNoLostValues()
            throws InterruptedException {
        final int threadCount = 4;
        final int keyCount = 100;
        final SenkuWriting<Integer, Long> writing = writing(new MemDirectory());
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threadCount);
        final ExecutorService callers = Executors.newFixedThreadPool(threadCount);
        try {
            for (int thread = 0; thread < threadCount; thread++) {
                callers.execute(() -> {
                    try {
                        start.await();
                        for (int key = 0; key < keyCount; key++) {
                            writing.put(key, 1L);
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
            callers.shutdownNow();
            assertTrue(callers.awaitTermination(10, TimeUnit.SECONDS));
        }

        final SenkuReady<Integer, Long> ready = writing.finishWriting();
        final List<Entry<Integer, Long>> expected = new ArrayList<>();
        for (int key = 0; key < keyCount; key++) {
            expected.add(Entry.of(key, (long) threadCount));
        }
        try (Stream<Entry<Integer, Long>> stream = ready.openStream()) {
            assertEquals(expected, stream.toList());
        }
        ready.close();
    }

    @Test
    void exactlyOneConcurrentFinishCallOwnsFinalization() throws Exception {
        final SenkuWriting<Integer, Long> writing = writing(new MemDirectory());
        writing.put(1, 1L);
        final CountDownLatch start = new CountDownLatch(1);
        final ExecutorService callers = Executors.newFixedThreadPool(2);
        try {
            final Future<SenkuReady<Integer, Long>> first = callers
                    .submit(() -> finishAfter(start, writing));
            final Future<SenkuReady<Integer, Long>> second = callers
                    .submit(() -> finishAfter(start, writing));
            start.countDown();

            final AtomicInteger failures = new AtomicInteger();
            final List<SenkuReady<Integer, Long>> readyHandles = new ArrayList<>();
            collectFinish(first, readyHandles, failures);
            collectFinish(second, readyHandles, failures);

            assertEquals(1, failures.get());
            assertEquals(1, readyHandles.size());
            readyHandles.get(0).close();
        } finally {
            callers.shutdownNow();
            assertTrue(callers.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void activeReadyHandleRejectsAnotherPublicOpen() {
        final MemDirectory directory = new MemDirectory();
        final SenkuReady<Integer, Long> ready = writing(directory)
                .finishWriting();

        assertThrows(IndexException.class,
                () -> SenkuIndex.open(directory, new TypeDescriptorInteger(),
                        new TypeDescriptorLong(), 1_024));

        ready.close();
    }

    @Test
    void maintenanceMayOverlapCallerIngestion() throws Exception {
        final CountDownLatch mergeStarted = new CountDownLatch(1);
        final CountDownLatch releaseMerge = new CountDownLatch(1);
        final SenkuWriting<Integer, Long> writing = writing(new MemDirectory(),
                1, 1, 2, 1, 1, blockingMerge(mergeStarted, releaseMerge));
        writing.put(1, 1L);
        writing.put(1, 2L);
        assertTrue(mergeStarted.await(10, TimeUnit.SECONDS));

        final ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            final Future<?> put = caller.submit(() -> writing.put(2, 2L));
            assertNull(put.get(10, TimeUnit.SECONDS));
        } finally {
            releaseMerge.countDown();
            caller.shutdownNow();
            assertTrue(caller.awaitTermination(10, TimeUnit.SECONDS));
        }

        writing.finishWriting().close();
        assertNoSenkuThreads();
    }

    @Test
    void queueSaturationPausesAndThenResumesIngestion() throws Exception {
        final CountDownLatch mergeStarted = new CountDownLatch(1);
        final CountDownLatch releaseMerge = new CountDownLatch(1);
        final SenkuWriting<Integer, Long> writing = writing(new MemDirectory(),
                4, 1, 2, 1, 1, blockingMerge(mergeStarted, releaseMerge));
        writing.put(0, 1L);
        writing.put(0, 2L);
        assertTrue(mergeStarted.await(10, TimeUnit.SECONDS));
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));

        final ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            final Future<?> put = caller.submit(() -> writing.put(2, 2L));
            assertThrows(TimeoutException.class,
                    () -> put.get(500, TimeUnit.MILLISECONDS));
            releaseMerge.countDown();
            assertNull(put.get(30, TimeUnit.SECONDS));
        } finally {
            releaseMerge.countDown();
            caller.shutdownNow();
            assertTrue(caller.awaitTermination(10, TimeUnit.SECONDS));
        }

        writing.finishWriting().close();
        assertNoSenkuThreads();
    }

    @Test
    void backgroundMergeFailureStopsEveryOwnedThread() {
        final SenkuWriting<Integer, Long> writing = writing(new MemDirectory(),
                1, 1, 2, 1, 1, (key, first, second) -> {
                    throw new IllegalStateException("injected merge failure");
                });
        writing.put(1, 1L);
        writing.put(1, 2L);

        assertThrows(IndexException.class, writing::finishWriting);
        assertThrows(IndexException.class, () -> writing.put(2, 2L));
        assertNoSenkuThreads();
    }

    @Test
    void readyCloseBreaksActiveStreamAndReleasesExclusiveLock() {
        final MemDirectory directory = new MemDirectory();
        final SenkuWriting<Integer, Long> writing = writing(directory);
        writing.put(2, 2L);
        writing.put(1, 1L);
        final SenkuReady<Integer, Long> ready = writing.finishWriting();
        final Stream<Entry<Integer, Long>> stream = ready.openStream();

        ready.close();

        assertEquals(List.of(), stream.toList());
        stream.close();
        SenkuIndex.open(directory, new TypeDescriptorInteger(),
                new TypeDescriptorLong(), 1_024).close();
        assertNoSenkuThreads();
    }

    private static SenkuReady<Integer, Long> finishAfter(
            final CountDownLatch start,
            final SenkuWriting<Integer, Long> writing) throws Exception {
        start.await();
        return writing.finishWriting();
    }

    private static void collectFinish(
            final Future<SenkuReady<Integer, Long>> future,
            final List<SenkuReady<Integer, Long>> readyHandles,
            final AtomicInteger failures) throws Exception {
        try {
            readyHandles.add(future.get(10, TimeUnit.SECONDS));
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IndexException);
            failures.incrementAndGet();
        }
    }

    private static SenkuWriting<Integer, Long> writing(
            final MemDirectory directory) {
        return writing(directory, 4, 1_000, 3, 4, 4,
                (key, first, second) -> first + second);
    }

    private static SenkuWriting<Integer, Long> writing(
            final MemDirectory directory, final int shardCount,
            final int maxInMemoryEntries, final int mergeFanIn,
            final int maintenanceThreads, final int maintenanceQueueSize,
            final SenkuMergeFunction<Integer, Long> mergeFunction) {
        final SenkuMergeFunctionRegistry<Integer, Long> functions =
                new SenkuMergeFunctionRegistry<>();
        functions.register(mergeFunction);
        return SenkuIndex
                .builder(directory, new TypeDescriptorInteger(),
                        new TypeDescriptorLong(), functions)
                .shardHashFunction(key -> key).shardCount(shardCount)
                .maxInMemoryEntries(maxInMemoryEntries).maxKeysPerPage(5)
                .mergeFanIn(mergeFanIn)
                .maintenanceThreads(maintenanceThreads)
                .maintenanceQueueSize(maintenanceQueueSize)
                .diskIoBufferSize(1_024).maxEntriesPerPart(10L).create();
    }

    private static SenkuMergeFunction<Integer, Long> blockingMerge(
            final CountDownLatch mergeStarted,
            final CountDownLatch releaseMerge) {
        return (key, first, second) -> {
            mergeStarted.countDown();
            try {
                releaseMerge.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IndexException("Interrupted injected merge.", e);
            }
            return first + second;
        };
    }

    private static void assertNoSenkuThreads() {
        final List<String> liveThreads = Thread.getAllStackTraces().keySet()
                .stream().filter(Thread::isAlive).map(Thread::getName)
                .filter(name -> name.startsWith("senku-coordinator-")
                        || name.startsWith("senku-maintenance-"))
                .sorted().toList();
        assertEquals(List.of(), liveThreads);
    }
}
