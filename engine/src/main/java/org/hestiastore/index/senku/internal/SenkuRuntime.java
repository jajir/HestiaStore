package org.hestiastore.index.senku.internal;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.ToIntFunction;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.senku.SenkuMergeFunction;
import org.hestiastore.index.senku.SenkuReady;
import org.hestiastore.index.senku.SenkuWriting;

/**
 * Internal cross-package bridge used only by the public Senku entry points.
 */
public final class SenkuRuntime {

    private SenkuRuntime() {
        // Static assembly bridge.
    }

    /**
     * Creates and starts one exclusive writing runtime.
     *
     * @param <K> key type
     * @param <V> value type
     * @return writing handle
     */
    public static <K, V> SenkuWriting<K, V> create(
            final Directory directory,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final SenkuMergeFunction<K, V> mergeFunction,
            final ToIntFunction<K> shardHashFunction, final int shardCount,
            final int maxInMemoryEntries, final int initialMapCapacity,
            final int maxKeysPerPage, final int mergeFanIn,
            final int maintenanceThreads, final int maintenanceQueueSize,
            final int diskIoBufferSize, final long maxEntriesPerPart) {
        final Directory root = Vldtn.requireNonNull(directory, "directory");
        final TypeDescriptor<K> keys = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        final TypeDescriptor<V> values = Vldtn.requireNonNull(
                valueTypeDescriptor, "valueTypeDescriptor");
        final SenkuMergeFunction<K, V> merge = Vldtn.requireNonNull(
                mergeFunction, "mergeFunction");
        final ToIntFunction<K> hash = Vldtn.requireNonNull(shardHashFunction,
                "shardHashFunction");
        final DataBlockSize blockSize = DataBlockSize.ofDataBlockSize(
                Vldtn.requireIoBufferSize(diskIoBufferSize,
                        "diskIoBufferSize"));
        FileLock fileLock = null;
        boolean lockAcquired = false;
        ThreadPoolExecutor workers = null;
        ScheduledExecutorService control = null;
        try {
            fileLock = root.getLock(SenkuFileNames.LOCK_FILE);
            fileLock.lock();
            lockAcquired = true;
            requireEmptyRoot(root);
            if (!root.mkdir(SenkuFileNames.FLUSH_DIRECTORY)) {
                throw new IndexException("Senku flush directory already exists.");
            }
            final Directory flush = root
                    .openSubDirectory(SenkuFileNames.FLUSH_DIRECTORY);
            final ReentrantLock writingLock = new ReentrantLock();
            final SenkuFlushWriter<K, V> flushWriter = new SenkuFlushWriter<>(
                    flush, keys, values, hash, shardCount, maxKeysPerPage,
                    maxEntriesPerPart, blockSize);
            final SenkuIngestor<K, V> ingestor = new SenkuIngestor<>(
                    new ReentrantLock(), merge, hash, flushWriter,
                    maxInMemoryEntries, initialMapCapacity);
            workers = new ThreadPoolExecutor(maintenanceThreads,
                    maintenanceThreads, 0L, TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(maintenanceQueueSize),
                    threadFactory("senku-maintenance-"),
                    SenkuRuntime::enqueueAfterWorkerHandoff);
            control = Executors.newSingleThreadScheduledExecutor(
                    threadFactory("senku-coordinator-"));
            final AtomicReference<IndexException> firstFailure =
                    new AtomicReference<>();
            final AtomicReference<SenkuWritingRuntime<K, V>> runtime =
                    new AtomicReference<>();
            final SenkuMaintenanceCoordinator<K, V> coordinator =
                    new SenkuMaintenanceCoordinator<>(root, shardCount,
                            mergeFanIn, keys, values, merge, maxKeysPerPage,
                            maxEntriesPerPart, blockSize, workers, control,
                            firstFailure, ingestor::setPaused,
                            failure -> runtime.get().backgroundFailure(failure),
                            () -> runtime.get().completionProcessed());
            final SenkuWritingRuntime<K, V> writing = new SenkuWritingRuntime<>(
                    root, keys, values, blockSize, shardCount, fileLock,
                    writingLock, ingestor, coordinator, control, workers,
                    firstFailure);
            runtime.set(writing);
            writing.start();
            return writing;
        } catch (Exception e) {
            cleanupCreateFailure(workers, control,
                    lockAcquired ? fileLock : null, e);
            throw asIndexException("Unable to create Senku index.", e);
        }
    }

    /**
     * Opens one exclusive ready handle without creating missing structure.
     *
     * @param <K> key type
     * @param <V> value type
     * @return ready handle
     */
    public static <K, V> SenkuReady<K, V> open(
            final Directory directory,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final int diskIoBufferSize) {
        final Directory root = Vldtn.requireNonNull(directory, "directory");
        final TypeDescriptor<K> keys = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        final TypeDescriptor<V> values = Vldtn.requireNonNull(
                valueTypeDescriptor, "valueTypeDescriptor");
        final DataBlockSize blockSize = DataBlockSize.ofDataBlockSize(
                Vldtn.requireIoBufferSize(diskIoBufferSize,
                        "diskIoBufferSize"));
        final FileLock fileLock = root.getLock(SenkuFileNames.LOCK_FILE);
        boolean lockAcquired = false;
        try {
            fileLock.lock();
            lockAcquired = true;
            return new SenkuReadyRuntime<>(root, keys, values, blockSize,
                    fileLock);
        } catch (Exception e) {
            if (lockAcquired) {
                try {
                    fileLock.unlock();
                } catch (Exception cleanupFailure) {
                    e.addSuppressed(cleanupFailure);
                }
            }
            throw asIndexException("Unable to open Senku index.", e);
        }
    }

    private static void requireEmptyRoot(final Directory root) {
        final Set<String> names = new HashSet<>(
                root.getFileNames().toList());
        names.remove(SenkuFileNames.LOCK_FILE);
        if (!names.isEmpty()) {
            throw new IndexException("Senku create requires an empty directory.");
        }
    }

    private static ThreadFactory threadFactory(final String prefix) {
        final AtomicInteger nextId = new AtomicInteger(1);
        return runnable -> {
            final Thread thread = new Thread(runnable,
                    prefix + nextId.getAndIncrement());
            thread.setDaemon(false);
            return thread;
        };
    }

    private static void enqueueAfterWorkerHandoff(final Runnable task,
            final ThreadPoolExecutor executor) {
        if (executor.isShutdown()) {
            throw new RejectedExecutionException(
                    "Senku maintenance executor is shut down.");
        }
        try {
            executor.getQueue().put(task);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RejectedExecutionException(
                    "Interrupted while enqueuing Senku maintenance.", e);
        }
    }

    private static void cleanupCreateFailure(final ThreadPoolExecutor workers,
            final ScheduledExecutorService control, final FileLock fileLock,
            final Exception primary) {
        if (workers != null) {
            workers.shutdownNow();
        }
        if (control != null) {
            control.shutdownNow();
        }
        if (fileLock != null) {
            try {
                fileLock.unlock();
            } catch (Exception cleanupFailure) {
                primary.addSuppressed(cleanupFailure);
            }
        }
    }

    private static IndexException asIndexException(final String message,
            final Exception cause) {
        return cause instanceof IndexException ? (IndexException) cause
                : new IndexException(message, cause);
    }
}
