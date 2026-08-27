package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.senku.internal.LargeFileTestSupport.DATA_BLOCK_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.senku.SenkuMergeFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SenkuWritingRuntimeConcurrencyTest {

    @Mock
    private Directory rootDirectory;

    @Mock
    private FileLock fileLock;

    @Mock
    private SenkuFlushWriter<Integer, Long> flushWriter;

    @Mock
    private SenkuMaintenanceCoordinator<Integer, Long> coordinator;

    @Mock
    private ScheduledExecutorService controlExecutor;

    @Mock
    private ThreadPoolExecutor workerExecutor;

    private final List<Long> generations = new CopyOnWriteArrayList<>();
    private final List<Map<Integer, Long>> batches =
            new CopyOnWriteArrayList<>();
    private final List<Integer> batchStripeCounts =
            new CopyOnWriteArrayList<>();

    private CountDownLatch flushStarted;
    private CountDownLatch releaseFlush;
    private ExecutorService callers;
    private SenkuIngestor<Integer, Long> ingestor;
    private SenkuWritingRuntime<Integer, Long> writing;

    @BeforeEach
    void setUp() {
        flushStarted = new CountDownLatch(1);
        releaseFlush = new CountDownLatch(1);
        callers = Executors.newFixedThreadPool(2);
        createRuntime((key, first, second) -> first + second, 2);
    }

    private void createRuntime(
            final SenkuMergeFunction<Integer, Long> mergeFunction,
            final int maxInMemoryEntries) {
        final ReentrantLock writingLock = new ReentrantLock();
        ingestor = new SenkuIngestor<>(new ReentrantLock(), mergeFunction,
                flushWriter, maxInMemoryEntries,
                maxInMemoryEntries * 2);
        writing = new SenkuWritingRuntime<>(rootDirectory,
                new TypeDescriptorInteger(), new TypeDescriptorLong(),
                DATA_BLOCK_SIZE, 1, fileLock, writingLock, ingestor,
                coordinator, controlExecutor, workerExecutor,
                new AtomicReference<>());
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        releaseFlush.countDown();
        callers.shutdownNow();
        assertTrue(callers.awaitTermination(5, TimeUnit.SECONDS));
    }

    @Test
    void runtimeAcceptsOneActiveBatchDuringFlushThenAppliesBackpressure()
            throws Exception {
        stubBlockingFlush();
        writing.put(1, 1L);
        final Future<?> firstFlush = callers.submit(() -> writing.put(2, 2L));
        try {
            assertTrue(flushStarted.await(5, TimeUnit.SECONDS));

            final Future<?> nextPut = callers.submit(() -> writing.put(3, 3L));
            nextPut.get(1, TimeUnit.SECONDS);
            final Future<?> fillsNextMap = callers
                    .submit(() -> writing.put(4, 4L));
            fillsNextMap.get(1, TimeUnit.SECONDS);
            final Future<?> blockedByFullNextMap = callers
                    .submit(() -> writing.put(5, 5L));

            assertEquals(2, ingestor.size());
            assertThrows(TimeoutException.class,
                    () -> blockedByFullNextMap.get(50,
                            TimeUnit.MILLISECONDS));
            releaseFlush.countDown();
            blockedByFullNextMap.get(5, TimeUnit.SECONDS);
        } finally {
            releaseFlush.countDown();
        }
        firstFlush.get(5, TimeUnit.SECONDS);

        assertEquals(List.of(0L, 1L), generations);
        assertEquals(List.of(Map.of(1, 1L, 2, 2L),
                Map.of(3, 3L, 4, 4L)), batches);
        assertEquals(List.of(32, 32), batchStripeCounts);
        verify(flushWriter, times(2)).write(anyLong(), anyList());
    }

    @Test
    void runtimePutsForDistinctKeysMutateConcurrently() throws Exception {
        final CountDownLatch mergesEntered = new CountDownLatch(2);
        final CountDownLatch releaseMerges = new CountDownLatch(1);
        createRuntime((key, first, second) -> {
            mergesEntered.countDown();
            try {
                if (!releaseMerges.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException(
                            "Timed out waiting for concurrent runtime merge.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "Concurrent runtime merge was interrupted.", e);
            }
            return first + second;
        }, 10);
        writing.put(1, 1L);
        writing.put(2, 1L);
        try {
            final Future<?> first = callers.submit(() -> writing.put(1, 1L));
            final Future<?> second = callers.submit(() -> writing.put(2, 1L));

            assertTrue(mergesEntered.await(1, TimeUnit.SECONDS));
            releaseMerges.countDown();
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
        } finally {
            releaseMerges.countDown();
        }

        assertEquals(2, ingestor.size());
    }

    @Test
    void stoppingWaitsForCurrentFlushThenWritesActiveBatch() throws Exception {
        stubBlockingFlush();
        writing.put(1, 1L);
        final Future<?> firstFlush = callers.submit(() -> writing.put(2, 2L));
        final CountDownLatch stopStarted = new CountDownLatch(1);
        Future<Boolean> stopping = null;
        try {
            assertTrue(flushStarted.await(5, TimeUnit.SECONDS));
            writing.put(3, 3L);
            stopping = callers.submit(() -> {
                stopStarted.countDown();
                return ingestor.stopAcceptingAndFlush();
            });
            assertTrue(stopStarted.await(5, TimeUnit.SECONDS));
            final Future<Boolean> blockedStop = stopping;

            assertThrows(TimeoutException.class,
                    () -> blockedStop.get(50, TimeUnit.MILLISECONDS));
        } finally {
            releaseFlush.countDown();
        }

        firstFlush.get(5, TimeUnit.SECONDS);
        assertTrue(stopping.get(5, TimeUnit.SECONDS));
        assertEquals(List.of(0L, 1L), generations);
        assertEquals(List.of(Map.of(1, 1L, 2, 2L), Map.of(3, 3L)), batches);
    }

    @Test
    void flushFailureStopsFurtherWrites() {
        final IndexException failure = new IndexException("flush failed");
        doThrow(failure).when(flushWriter).write(anyLong(), anyList());
        writing.put(1, 1L);

        final IndexException actual = assertThrows(IndexException.class,
                () -> writing.put(2, 2L));

        assertSame(failure, actual);
        assertEquals(SenkuWritingState.ERROR, writing.state());
        assertThrows(IndexException.class, () -> writing.put(3, 3L));
    }

    private void stubBlockingFlush() {
        doAnswer(invocation -> {
            final long generation = invocation.getArgument(0);
            final List<Map<Integer, Long>> entries = invocation.getArgument(1);
            generations.add(generation);
            batchStripeCounts.add(entries.size());
            if (generation == 0L) {
                flushStarted.countDown();
                assertTrue(releaseFlush.await(5, TimeUnit.SECONDS));
            }
            final Map<Integer, Long> batch = new HashMap<>();
            entries.forEach(batch::putAll);
            batches.add(Map.copyOf(batch));
            return null;
        }).when(flushWriter).write(anyLong(), anyList());
    }
}
