package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.senku.internal.LargeFileTestSupport.DATA_BLOCK_SIZE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorList;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.senku.SenkuMergeFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SenkuMaintenanceCoordinatorTest {

    private MemDirectory root;
    private Directory flushDirectory;
    private ThreadPoolExecutor workers;
    private ExecutorService control;

    @BeforeEach
    void setUp() {
        root = new MemDirectory();
        root.mkdir(SenkuFileNames.FLUSH_DIRECTORY);
        flushDirectory = root
                .openSubDirectory(SenkuFileNames.FLUSH_DIRECTORY);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (workers != null) {
            workers.shutdownNow();
            assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
        }
        if (control != null) {
            control.shutdownNow();
            assertTrue(control.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void scanDiscoversCommittedMetadataAndIgnoresIncompleteSources() {
        committedFlush(0L, 1);
        flushDirectory.mkdir(SenkuFileNames.flushDirectory(1L));
        committedRun(0, 0, 0L, new SenkuRunManifest(0, 0L));
        incompleteRun(1, 0, 0L);
        root.touch(SenkuFileNames.LOCK_FILE);
        final SenkuMaintenanceCoordinator<?, ?> coordinator = coordinator(2,
                2);

        coordinator.scanOnce();

        assertEquals(1, coordinator.flushCount());
        assertEquals(1, coordinator.runCount());
        assertArrayEquals(new long[] { 0L },
                coordinator.eligibleFlushIds(true));
    }

    @Test
    void scanDoesNotEnumerateOrValidateRunDataParts() {
        final Directory run = committedRun(0, 0, 0L,
                new SenkuRunManifest(0, 0L));
        run.touch(SenkuFileNames.partFile(0));
        final SenkuMaintenanceCoordinator<?, ?> coordinator = coordinator(1,
                2);

        coordinator.scanOnce();

        assertEquals(1, coordinator.runCount());
    }

    @Test
    void repeatedScanIsStableAndMissingKnownSourceFailsFast() {
        final Directory run = committedRun(0, 0, 0L,
                new SenkuRunManifest(0, 0L));
        final SenkuMaintenanceCoordinator<?, ?> coordinator = coordinator(1,
                2);
        coordinator.scanOnce();
        coordinator.scanOnce();
        run.deleteFile(SenkuFileNames.MANIFEST_FILE);

        assertThrows(IndexException.class, coordinator::scanOnce);
    }

    @Test
    void changedAuthoritativeManifestFailsFast() {
        final Directory run = committedRun(0, 0, 0L,
                new SenkuRunManifest(0, 0L));
        final SenkuMaintenanceCoordinator<?, ?> coordinator = coordinator(1,
                2);
        coordinator.scanOnce();
        run.deleteFile(SenkuFileNames.MANIFEST_FILE);
        SenkuMetadataCodec.publishRunManifest(run,
                new SenkuRunManifest(1, 1L));

        assertThrows(IndexException.class, coordinator::scanOnce);
    }

    @Test
    void malformedOrOutOfRangeStructuralNameFailsFast() {
        root.mkdir("unknown");
        assertThrows(IndexException.class, coordinator(1, 2)::scanOnce);

        final MemDirectory otherRoot = new MemDirectory();
        otherRoot.mkdir(SenkuFileNames.FLUSH_DIRECTORY);
        otherRoot.mkdir(SenkuFileNames.shardDirectory(2));
        assertThrows(IndexException.class,
                new SenkuMaintenanceCoordinator<>(otherRoot, 1, 2)::scanOnce);
    }

    @Test
    void eligibleRunGroupUsesBottomUpDeterministicOrder() {
        committedRun(1, 0, 3L, new SenkuRunManifest(0, 0L));
        committedRun(1, 0, 1L, new SenkuRunManifest(0, 0L));
        committedRun(0, 1, 0L, new SenkuRunManifest(0, 0L));
        final SenkuMaintenanceCoordinator<?, ?> coordinator = coordinator(2,
                2);
        coordinator.scanOnce();

        final List<SenkuRunSource> selected = coordinator
                .eligibleRunSources(false);

        assertEquals(List.of(1L, 3L),
                selected.stream().map(SenkuRunSource::runId).toList());
    }

    @Test
    void boundedSchedulerAcceptsCompleteL0BatchThenDeletesFlushInputs() {
        writeFlush(2, 0L, entries(0, 1L, 1, 2L));
        writeFlush(2, 1L, entries(0, 4L, 3, 8L));
        final AtomicReference<IndexException> failure = new AtomicReference<>();
        final SenkuMaintenanceCoordinator<Integer, Long> coordinator =
                scheduler(2, 2, 2, 2,
                        (key, first, second) -> first + second, failure,
                        ignored -> {
                            // No backpressure assertion in this test.
                        });

        coordinator.scanAndScheduleOnce(false);
        await(() -> !coordinator.hasActiveL0Batch()
                || failure.get() != null);

        assertEquals(null, failure.get());
        assertEquals(0, coordinator.flushCount());
        assertEquals(2, coordinator.runCount());
        assertEquals(List.of(), flushDirectory.getFileNames().toList());
        assertTrue(root.isFileExists(SenkuFileNames.shardDirectory(0)));
        assertTrue(root.isFileExists(SenkuFileNames.shardDirectory(1)));
    }

    @Test
    void queueFullWithPendingL0ShardPausesThenResumesIngestion() {
        writeFlush(3, 0L, entries(0, 1L, 1, 1L, 2, 1L));
        writeFlush(3, 1L, entries(0, 2L, 1, 2L, 2, 2L));
        final CountDownLatch mergeEntered = new CountDownLatch(1);
        final CountDownLatch releaseMerge = new CountDownLatch(1);
        final Queue<Boolean> pauses = new ConcurrentLinkedQueue<>();
        final AtomicReference<IndexException> failure = new AtomicReference<>();
        final SenkuMergeFunction<Integer, Long> blockingMerge =
                (key, first, second) -> {
                    mergeEntered.countDown();
                    awaitLatch(releaseMerge);
                    return first + second;
                };
        final SenkuMaintenanceCoordinator<Integer, Long> coordinator =
                scheduler(3, 2, 1, 1, blockingMerge, failure, pauses::add);

        coordinator.scanAndScheduleOnce(false);
        awaitLatch(mergeEntered);

        assertEquals(2, coordinator.submittedCount());
        assertEquals(Boolean.TRUE, pauses.peek());
        releaseMerge.countDown();
        await(() -> coordinator.submittedCount() == 0
                || failure.get() != null);
        assertTrue(coordinator.hasActiveL0Batch());

        coordinator.scanAndScheduleOnce(false);
        await(() -> !coordinator.hasActiveL0Batch()
                || failure.get() != null);

        assertEquals(null, failure.get());
        assertEquals(Boolean.FALSE, pauses.stream().reduce((a, b) -> b)
                .orElseThrow());
    }

    @Test
    void schedulerDoesNotQueueTwoSortedRunMergesForOneShard() {
        writeRun(0, 0, 0L, Entry.of(1, 1L));
        writeRun(0, 0, 1L, Entry.of(1, 2L));
        writeRun(0, 1, 0L, Entry.of(2, 1L));
        writeRun(0, 1, 1L, Entry.of(2, 2L));
        final CountDownLatch mergeEntered = new CountDownLatch(1);
        final CountDownLatch releaseMerge = new CountDownLatch(1);
        final AtomicReference<IndexException> failure = new AtomicReference<>();
        final SenkuMaintenanceCoordinator<Integer, Long> coordinator =
                scheduler(1, 2, 2, 2, (key, first, second) -> {
                    mergeEntered.countDown();
                    awaitLatch(releaseMerge);
                    return first + second;
                }, failure, ignored -> {
                    // Backpressure is covered separately.
                });

        coordinator.scanAndScheduleOnce(false);
        awaitLatch(mergeEntered);

        assertEquals(1, coordinator.submittedCount());
        releaseMerge.countDown();
        await(() -> coordinator.submittedCount() == 0
                || failure.get() != null);
        assertEquals(null, failure.get());
    }

    private SenkuMaintenanceCoordinator<?, ?> coordinator(final int shardCount,
            final int mergeFanIn) {
        return new SenkuMaintenanceCoordinator<>(root, shardCount, mergeFanIn);
    }

    private SenkuMaintenanceCoordinator<Integer, Long> scheduler(
            final int shardCount, final int mergeFanIn,
            final int maintenanceThreads, final int queueSize,
            final SenkuMergeFunction<Integer, Long> mergeFunction,
            final AtomicReference<IndexException> failure,
            final Consumer<Boolean> pauseConsumer) {
        workers = new ThreadPoolExecutor(maintenanceThreads, maintenanceThreads,
                0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(queueSize));
        control = Executors.newSingleThreadExecutor();
        return new SenkuMaintenanceCoordinator<>(root, shardCount, mergeFanIn,
                new TypeDescriptorInteger(), new TypeDescriptorLong(),
                mergeFunction, 1, 4L, DATA_BLOCK_SIZE, workers, control,
                failure, pauseConsumer);
    }

    private void committedFlush(final long flushId, final int partCount) {
        final String name = SenkuFileNames.flushDirectory(flushId);
        flushDirectory.mkdir(name);
        SenkuMetadataCodec.publishFlushManifest(
                flushDirectory.openSubDirectory(name), partCount);
    }

    private Directory committedRun(final int shardId, final int level,
            final long runId, final SenkuRunManifest manifest) {
        final Directory run = incompleteRun(shardId, level, runId);
        SenkuMetadataCodec.publishRunManifest(run, manifest);
        return run;
    }

    private Directory incompleteRun(final int shardId, final int level,
            final long runId) {
        final String shardName = SenkuFileNames.shardDirectory(shardId);
        if (!root.isFileExists(shardName)) {
            root.mkdir(shardName);
        }
        final Directory shard = root.openSubDirectory(shardName);
        final String levelName = SenkuFileNames.levelDirectory(level);
        if (!shard.isFileExists(levelName)) {
            shard.mkdir(levelName);
        }
        final Directory levelDirectory = shard.openSubDirectory(levelName);
        final String runName = SenkuFileNames.runDirectory(runId);
        if (!levelDirectory.isFileExists(runName)) {
            levelDirectory.mkdir(runName);
        }
        return levelDirectory.openSubDirectory(runName);
    }

    private void writeFlush(final int shardCount, final long generation,
            final Map<Integer, Long> entries) {
        new SenkuFlushWriter<>(flushDirectory, new TypeDescriptorInteger(),
                new TypeDescriptorLong(), key -> key, shardCount, 1, 4L,
                DATA_BLOCK_SIZE).write(generation, entries);
    }

    @SafeVarargs
    private final void writeRun(final int shardId, final int level,
            final long runId, final Entry<Integer, Long>... entries) {
        final Directory run = incompleteRun(shardId, level, runId);
        new SenkuRunWriter<>(run, new TypeDescriptorInteger(),
                new TypeDescriptorLong(), 1, 4L, DATA_BLOCK_SIZE)
                .write(new EntryIteratorList<>(List.of(entries)));
    }

    private static Map<Integer, Long> entries(final int firstKey,
            final long firstValue, final int secondKey,
            final long secondValue) {
        return entries(firstKey, firstValue, secondKey, secondValue,
                Integer.MIN_VALUE, 0L);
    }

    private static Map<Integer, Long> entries(final int firstKey,
            final long firstValue, final int secondKey, final long secondValue,
            final int thirdKey, final long thirdValue) {
        final Map<Integer, Long> entries = new LinkedHashMap<>();
        entries.put(firstKey, firstValue);
        entries.put(secondKey, secondValue);
        if (thirdKey != Integer.MIN_VALUE) {
            entries.put(thirdKey, thirdValue);
        }
        return entries;
    }

    private static void await(final BooleanSupplier condition) {
        final long deadline = System.nanoTime()
                + Duration.ofSeconds(5).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        assertTrue(condition.getAsBoolean(), "Timed out waiting for condition");
    }

    private static void awaitLatch(final CountDownLatch latch) {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    assertTrue(latch.await(5, TimeUnit.SECONDS));
                    return;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
