package org.hestiastore.index.senku.internal;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.senku.SenkuReady;
import org.hestiastore.index.senku.SenkuWriting;

/**
 * Owns the writing lifecycle, maintenance executors, and root lock transfer.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SenkuWritingRuntime<K, V> implements SenkuWriting<K, V> {

    private static final long SCAN_INTERVAL_SECONDS = 3L;

    private final Directory rootDirectory;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final DataBlockSize dataBlockSize;
    private final int shardCount;
    private final FileLock fileLock;
    private final ReentrantLock writingLock;
    private final SenkuIngestor<K, V> ingestor;
    private final SenkuMaintenanceCoordinator<K, V> coordinator;
    private final ScheduledExecutorService controlExecutor;
    private final ThreadPoolExecutor workerExecutor;
    private final AtomicReference<IndexException> firstFailure;
    private final CountDownLatch maintenanceFinished = new CountDownLatch(1);
    private final AtomicBoolean terminalized = new AtomicBoolean();

    private volatile SenkuWritingState state = SenkuWritingState.WRITING;
    private volatile SenkuReadyRuntime<K, V> ready;
    private ScheduledFuture<?> periodicScan;

    SenkuWritingRuntime(final Directory rootDirectory,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final DataBlockSize dataBlockSize, final int shardCount,
            final FileLock fileLock, final ReentrantLock writingLock,
            final SenkuIngestor<K, V> ingestor,
            final SenkuMaintenanceCoordinator<K, V> coordinator,
            final ScheduledExecutorService controlExecutor,
            final ThreadPoolExecutor workerExecutor,
            final AtomicReference<IndexException> firstFailure) {
        this.rootDirectory = Vldtn.requireNonNull(rootDirectory,
                "rootDirectory");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.dataBlockSize = Vldtn.requireNonNull(dataBlockSize,
                "dataBlockSize");
        this.shardCount = Vldtn.requireGreaterThanZero(shardCount,
                "shardCount");
        this.fileLock = Vldtn.requireNonNull(fileLock, "fileLock");
        this.writingLock = Vldtn.requireNonNull(writingLock, "writingLock");
        this.ingestor = Vldtn.requireNonNull(ingestor, "ingestor");
        this.coordinator = Vldtn.requireNonNull(coordinator, "coordinator");
        this.controlExecutor = Vldtn.requireNonNull(controlExecutor,
                "controlExecutor");
        this.workerExecutor = Vldtn.requireNonNull(workerExecutor,
                "workerExecutor");
        this.firstFailure = Vldtn.requireNonNull(firstFailure, "firstFailure");
    }

    /**
     * Starts the three-second metadata scan only after complete construction.
     */
    void start() {
        periodicScan = controlExecutor.scheduleWithFixedDelay(this::tickSafely,
                SCAN_INTERVAL_SECONDS, SCAN_INTERVAL_SECONDS,
                TimeUnit.SECONDS);
    }

    @Override
    public void put(final K key, final V value) {
        IndexException failure = null;
        writingLock.lock();
        try {
            Vldtn.requireNonNull(key, "key");
            Vldtn.requireNonNull(value, "value");
            requireWriting("put");
            try {
                ingestor.put(key, value);
            } catch (IndexException e) {
                failure = e;
            }
        } finally {
            writingLock.unlock();
        }
        if (failure != null) {
            reportCallerFailure(failure);
            throw failure;
        }
    }

    @Override
    public SenkuReady<K, V> finishWriting() {
        IndexException callerFailure = null;
        writingLock.lock();
        try {
            requireWriting("finishWriting");
            state = SenkuWritingState.FINISHING;
            try {
                ingestor.stopAcceptingAndFlush();
            } catch (IndexException e) {
                callerFailure = e;
                state = SenkuWritingState.ERROR;
            }
        } finally {
            writingLock.unlock();
        }
        if (callerFailure != null) {
            reportCallerFailure(callerFailure);
        } else {
            wakeCoordinator();
        }
        awaitMaintenanceFinished();
        awaitControlTermination();
        if (firstFailure.get() != null || ready == null) {
            throw new IndexException("Unable to finish Senku writing.");
        }
        return ready;
    }

    /**
     * Moves the writing gate to ERROR and wakes failure shutdown.
     *
     * @param failure first background failure
     */
    void backgroundFailure(final IndexException failure) {
        Vldtn.requireNonNull(failure, "failure");
        writingLock.lock();
        try {
            if (state != SenkuWritingState.TRANSFERRED) {
                state = SenkuWritingState.ERROR;
                ingestor.fail();
            }
        } finally {
            writingLock.unlock();
        }
        wakeCoordinator();
    }

    /**
     * Checks only terminal drain completion after a worker result.
     */
    void completionProcessed() {
        if (state == SenkuWritingState.FINISHING
                && coordinator.isDrainComplete()) {
            completeSuccess();
        }
    }

    SenkuWritingState state() {
        return state;
    }

    private void tickSafely() {
        if (terminalized.get()) {
            return;
        }
        try {
            if (firstFailure.get() != null
                    || state == SenkuWritingState.ERROR) {
                shutdownFailure();
                return;
            }
            final boolean drain = state == SenkuWritingState.FINISHING;
            coordinator.scanAndScheduleOnce(drain);
            if (firstFailure.get() != null) {
                shutdownFailure();
            } else if (drain && coordinator.isDrainComplete()) {
                completeSuccess();
            }
        } catch (Exception e) {
            final IndexException failure = asIndexException(
                    "Senku coordinator tick failed.", e);
            if (firstFailure.compareAndSet(null, failure)) {
                backgroundFailure(failure);
            } else {
                shutdownFailure();
            }
        }
    }

    private void completeSuccess() {
        if (!terminalized.compareAndSet(false, true)) {
            return;
        }
        try {
            workerExecutor.shutdown();
            awaitWorkers();
            SenkuMetadataCodec.publishReady(rootDirectory, shardCount);
            ready = new SenkuReadyRuntime<>(rootDirectory, keyTypeDescriptor,
                    valueTypeDescriptor, dataBlockSize, fileLock);
            writingLock.lock();
            try {
                state = SenkuWritingState.TRANSFERRED;
            } finally {
                writingLock.unlock();
            }
            finishControl();
        } catch (Exception e) {
            terminalized.set(false);
            final IndexException failure = asIndexException(
                    "Unable to publish the ready Senku index.", e);
            firstFailure.compareAndSet(null, failure);
            backgroundFailure(failure);
        }
    }

    private void shutdownFailure() {
        if (!terminalized.compareAndSet(false, true)) {
            return;
        }
        cancelPeriodicScan();
        workerExecutor.getQueue().clear();
        workerExecutor.shutdown();
        awaitWorkers();
        final IndexException primary = firstFailure.get();
        try {
            fileLock.unlock();
        } catch (Exception cleanupFailure) {
            if (primary != null) {
                primary.addSuppressed(cleanupFailure);
            }
        }
        finishControl();
    }

    private void finishControl() {
        cancelPeriodicScan();
        controlExecutor.shutdown();
        maintenanceFinished.countDown();
    }

    private void reportCallerFailure(final IndexException failure) {
        if (firstFailure.compareAndSet(null, failure)) {
            backgroundFailure(failure);
        } else {
            wakeCoordinator();
        }
    }

    private void wakeCoordinator() {
        if (controlExecutor.isShutdown()) {
            return;
        }
        try {
            controlExecutor.execute(this::tickSafely);
        } catch (RejectedExecutionException e) {
            if (!terminalized.get()) {
                final IndexException failure = new IndexException(
                        "Unable to wake Senku coordinator.", e);
                firstFailure.compareAndSet(null, failure);
            }
        }
    }

    private void requireWriting(final String operation) {
        if (state != SenkuWritingState.WRITING) {
            throw new IndexException("Senku " + operation
                    + " is not allowed in state " + state + ".");
        }
    }

    private void cancelPeriodicScan() {
        final ScheduledFuture<?> scan = periodicScan;
        if (scan != null) {
            scan.cancel(false);
        }
    }

    private void awaitWorkers() {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    if (workerExecutor.awaitTermination(1, TimeUnit.DAYS)) {
                        return;
                    }
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

    private void awaitMaintenanceFinished() {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    maintenanceFinished.await();
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

    private void awaitControlTermination() {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    if (controlExecutor.awaitTermination(1, TimeUnit.DAYS)) {
                        return;
                    }
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

    private static IndexException asIndexException(final String message,
            final Exception cause) {
        return cause instanceof IndexException ? (IndexException) cause
                : new IndexException(message, cause);
    }
}
