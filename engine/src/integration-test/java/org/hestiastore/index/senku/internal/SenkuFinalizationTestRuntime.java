package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.hestiastore.index.Entry;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.senku.SenkuMergeFunction;
import org.hestiastore.index.senku.SenkuReady;

/**
 * Assembles actual Senku persistence and lifecycle components with manually
 * advanced maintenance executors. Every test owns its directory and caller.
 */
final class SenkuFinalizationTestRuntime implements AutoCloseable {

    static final TypeDescriptorInteger KEYS = new TypeDescriptorInteger();
    static final TypeDescriptorLong VALUES = new TypeDescriptorLong();
    static final int BLOCK_BYTES = 1024;
    private static final DataBlockSize BLOCK = DataBlockSize
            .ofDataBlockSize(BLOCK_BYTES);
    private static final int MAX_ENTRIES_PER_PART = 2;

    final Directory root;
    final Directory flush;
    final SenkuManualControlExecutor control;
    final SenkuManualWorkerExecutor workers;
    final SenkuMaintenanceCoordinator<Integer, Long> coordinator;
    final SenkuWritingRuntime<Integer, Long> writing;
    final AtomicReference<IndexException> firstFailure;

    private final int shardCount;
    private final CompletableFuture<SenkuReady<Integer, Long>> finish = new CompletableFuture<>();
    private Thread finisher;

    private SenkuFinalizationTestRuntime(final Directory root,
            final Directory flush, final int shardCount,
            final SenkuManualControlExecutor control,
            final SenkuManualWorkerExecutor workers,
            final SenkuMaintenanceCoordinator<Integer, Long> coordinator,
            final SenkuWritingRuntime<Integer, Long> writing,
            final AtomicReference<IndexException> firstFailure) {
        this.root = root;
        this.flush = flush;
        this.shardCount = shardCount;
        this.control = control;
        this.workers = workers;
        this.coordinator = coordinator;
        this.writing = writing;
        this.firstFailure = firstFailure;
    }

    /**
     * Creates the production object graph with deterministic task admission.
     *
     * @param root       isolated test directory
     * @param shardCount persistent shard count
     * @param threshold  automatic ingestion flush threshold
     * @param merge      duplicate-key reduction used by actual merges
     * @return started runtime owned by the test
     */
    static SenkuFinalizationTestRuntime createStarted(final Directory root,
            final int shardCount, final int threshold,
            final SenkuMergeFunction<Integer, Long> merge) {
        final FileLock fileLock = root.getLock(SenkuFileNames.LOCK_FILE);
        fileLock.lock();
        final SenkuStorageFormat format = SenkuStorageFormat.createDefault();
        SenkuMetadataCodec.publishStorageFormat(root, format);
        root.mkdir(SenkuFileNames.FLUSH_DIRECTORY);
        final Directory flush = root
                .openSubDirectory(SenkuFileNames.FLUSH_DIRECTORY);
        final SenkuManualControlExecutor control = new SenkuManualControlExecutor();
        final SenkuManualWorkerExecutor workers = new SenkuManualWorkerExecutor();
        final AtomicReference<IndexException> firstFailure = new AtomicReference<>();
        final AtomicReference<SenkuWritingRuntime<Integer, Long>> runtime = new AtomicReference<>();
        final SenkuFlushWriter<Integer, Long> writer = new SenkuFlushWriter<>(
                flush, KEYS, VALUES, key -> key, shardCount, 1,
                MAX_ENTRIES_PER_PART, BLOCK, format);
        final SenkuIngestor<Integer, Long> ingestor = new SenkuIngestor<>(
                new ReentrantLock(), merge, key -> key, writer, threshold, 4);
        final SenkuMaintenanceCoordinator<Integer, Long> coordinator = new SenkuMaintenanceCoordinator<>(
                root, shardCount, 2, KEYS, VALUES, merge, 1,
                MAX_ENTRIES_PER_PART, BLOCK, workers, control, firstFailure,
                ingestor::setPaused,
                failure -> runtime.get().backgroundFailure(failure),
                () -> runtime.get().completionProcessed(), format);
        final SenkuWritingRuntime<Integer, Long> writing = new SenkuWritingRuntime<>(
                root, KEYS, VALUES, BLOCK, shardCount, fileLock,
                new ReentrantLock(), ingestor, coordinator, control, workers,
                firstFailure);
        runtime.set(writing);
        writing.start();
        return new SenkuFinalizationTestRuntime(root, flush, shardCount,
                control, workers, coordinator, writing, firstFailure);
    }

    /**
     * Starts the real blocking finish call. Taking its submitted task proves
     * that ingestion stopped and the FINISHING transition already occurred.
     *
     * @return finalization scan, held until the test executes it
     * @throws InterruptedException when interrupted while awaiting submission
     */
    Runnable startFinish() throws InterruptedException {
        finisher = new Thread(() -> {
            try {
                finish.complete(writing.finishWriting());
            } catch (Exception failure) {
                finish.completeExceptionally(failure);
            }
        }, "senku-deterministic-finisher");
        finisher.start();
        return control.takeTask();
    }

    /**
     * Executes the currently reserved jobs and holds their control callbacks.
     * No later scan can intervene before the test releases those callbacks.
     *
     * @return callbacks in their actual submission order
     * @throws InterruptedException when interrupted while awaiting a callback
     */
    List<Runnable> finishReservedJobs() throws InterruptedException {
        final List<Runnable> completions = new ArrayList<>();
        while (workers.hasPendingTasks()) {
            workers.runNextTask();
            completions.add(control.takeTask());
        }
        return completions;
    }

    /**
     * Advances actual merges and their queued callbacks until the production
     * runtime terminates, without another periodic scan. Finishing completions
     * must schedule the remaining work themselves. The bound detects livelock,
     * not timing.
     *
     * @throws InterruptedException when interrupted while awaiting a callback
     */
    void drain() throws InterruptedException {
        for (int step = 0; step < 32 && !control.isShutdown(); step++) {
            while (workers.hasPendingTasks()) {
                workers.runNextTask();
            }
            while (control.hasPendingTasks() && !control.isShutdown()) {
                control.takeTask().run();
            }
            assertTrue(
                    control.isShutdown() || workers.hasPendingTasks()
                            || control.hasPendingTasks(),
                    "Finalization stalled waiting for an unscheduled scan.");
        }
        assertTrue(control.isShutdown(),
                "Finalization must terminate within 32 scheduling steps.");
    }

    /**
     * Awaits the already-driven finish caller with a deadlock watchdog.
     *
     * @return real ready handle
     * @throws InterruptedException when interrupted while waiting
     * @throws ExecutionException   when production finalization failed
     * @throws TimeoutException     when the finish caller did not terminate
     */
    SenkuReady<Integer, Long> awaitReady()
            throws InterruptedException, ExecutionException, TimeoutException {
        return finish.get(10, TimeUnit.SECONDS);
    }

    List<String> flushNames() {
        try (var names = flush.getFileNames()) {
            return names.sorted().toList();
        }
    }

    /**
     * Decodes an actual retained flush, including all persistent shards.
     *
     * @param generation flush generation
     * @return decoded entries in key order
     */
    List<Entry<Integer, Long>> readFlush(final long generation) {
        final Directory source = flush
                .openSubDirectory(SenkuFileNames.flushDirectory(generation));
        final SenkuShardIndex index = SenkuShardIndexCodec.read(source, BLOCK,
                shardCount);
        final LargeFile file = new LargeFile(source, BLOCK,
                MAX_ENTRIES_PER_PART,
                SenkuMetadataCodec.readFlushPartCount(source));
        final List<Entry<Integer, Long>> entries = new ArrayList<>();
        for (int shard = 0; shard < shardCount; shard++) {
            if (index.recordCount(shard) > 0) {
                final SenkuMergeSource range = SenkuMergeSource.flush(file,
                        LargeFilePosition
                                .fromPacked(index.packedPosition(shard)),
                        index.recordCount(shard));
                try (var iterator = range.open(KEYS, VALUES)) {
                    while (iterator.hasNext()) {
                        entries.add(iterator.next());
                    }
                }
            }
        }
        entries.sort(Comparator.comparing(Entry::getKey));
        return entries;
    }

    /**
     * Ensures a failed assertion cannot strand the blocking finish caller.
     * Production failure cleanup still owns executor and root-lock release.
     */
    @Override
    public void close() {
        try {
            if (!control.isShutdown()) {
                final IndexException cleanupFailure = new IndexException(
                        "Stopping unfinished deterministic test runtime.");
                firstFailure.compareAndSet(null, cleanupFailure);
                writing.backgroundFailure(cleanupFailure);
                while (control.hasPendingTasks() && !control.isShutdown()) {
                    control.takeTask().run();
                }
            }
            if (finisher != null) {
                finisher.join(TimeUnit.SECONDS.toMillis(10));
                assertFalse(finisher.isAlive(),
                        "Finish caller leaked after cleanup.");
            }
            if (finish.isDone() && !finish.isCompletedExceptionally()) {
                finish.join().close();
            }
            assertTrue(control.isTerminated(),
                    "Control executor did not terminate.");
            assertTrue(workers.isTerminated(),
                    "Worker executor did not terminate.");
            assertTrue(control.isPeriodicScanCancelled(),
                    "Periodic scan was not cancelled.");
            assertFalse(root.isFileExists(SenkuFileNames.LOCK_FILE),
                    "Root lock leaked.");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(
                    "Interrupted during deterministic runtime cleanup.",
                    interrupted);
        } finally {
            workers.shutdownNow();
            control.shutdownNow();
        }
    }
}
