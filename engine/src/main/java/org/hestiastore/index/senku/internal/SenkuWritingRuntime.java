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
    private final AtomicBoolean completionWakePending = new AtomicBoolean();

    private volatile SenkuWritingState state = SenkuWritingState.WRITING;
    private volatile SenkuReadyRuntime<K, V> ready;
    private volatile boolean finishRequested;
    private ScheduledFuture<?> periodicScan;
    // Accessed only by the serialized control executor. No WRITING tick may
    // establish this condition, even if finish starts while that tick runs.
    private boolean finishingReconciled;

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
                SCAN_INTERVAL_SECONDS, SCAN_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(final K key, final V value) {
        Vldtn.requireNonNull(key, "key");
        Vldtn.requireNonNull(value, "value");
        requireWriting("put");
        try {
            ingestor.put(key, value);
        } catch (IndexException e) {
            if (finishRequested || state != SenkuWritingState.WRITING) {
                requireWriting("put");
            }
            reportCallerFailure(e);
            throw e;
        }
    }

    /**
     * Accepts a primitive set key through this runtime's existing writing gate.
     *
     * @param key exact primitive key
     */
    void putLong(final long key) {
        requireWriting("put");
        try {
            ingestor.putLong(key);
        } catch (IndexException e) {
            if (finishRequested || state != SenkuWritingState.WRITING) {
                requireWriting("put");
            }
            reportCallerFailure(e);
            throw e;
        }
    }

    /**
     * Validates a complete primitive slice before entering the existing writing
     * gate. Operational failures preserve the same first-failure ownership as
     * individual puts; argument errors do not poison the writer.
     *
     * @param keys   caller-owned keys, not retained after return
     * @param offset first selected key
     * @param length selected key count
     */
    void putLongs(final long[] keys, final int offset, final int length) {
        SenkuLongBatch.validateSlice(keys, offset, length);
        requireWriting("putLongs");
        try {
            ingestor.putLongs(keys, offset, length);
        } catch (IndexException e) {
            if (finishRequested || state != SenkuWritingState.WRITING) {
                requireWriting("putLongs");
            }
            reportCallerFailure(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SenkuReady<K, V> finishWriting() {
        IndexException callerFailure = null;
        writingLock.lock();
        try {
            requireWriting("finishWriting");
            finishRequested = true;
            try {
                ingestor.stopAcceptingAndFlush();
                state = SenkuWritingState.FINISHING;
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
        return completedResult();
    }

    /**
     * Returns the transferred ready handle or rethrows the recorded first
     * failure without replacing its cause chain.
     *
     * @return transferred ready handle
     */
    SenkuReady<K, V> completedResult() {
        final IndexException failure = firstFailure.get();
        if (failure != null) {
            throw failure;
        }
        final SenkuReady<K, V> completed = ready;
        if (completed == null) {
            throw new IndexException("Unable to finish Senku writing.");
        }
        return completed;
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
     * Coalesces completion-driven scheduling on the serialized control
     * executor. Writing completions refill capacity and refresh backpressure
     * without a metadata scan. Finishing still requires fresh strict
     * reconciliation after ingestion stops and again before readiness can be
     * published.
     */
    void completionProcessed() {
        if (terminalized.get() || controlExecutor.isShutdown()
                || !completionWakePending.compareAndSet(false, true)) {
            return;
        }
        try {
            controlExecutor.execute(() -> {
                completionWakePending.set(false);
                maintainSafely(false);
            });
        } catch (RejectedExecutionException e) {
            completionWakePending.set(false);
            recordWakeFailure(e);
        }
    }

    SenkuWritingState state() {
        return state;
    }

    private void tickSafely() {
        maintainSafely(true);
    }

    /**
     * Runs discovery or completion scheduling under the sole lifecycle owner.
     */
    private void maintainSafely(final boolean discover) {
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
            if (drain && !finishingReconciled) {
                coordinator.scanAndScheduleOnce(true);
                finishingReconciled = true;
            } else if (discover) {
                coordinator.discoverAndScheduleOnce(drain);
            } else {
                coordinator.scheduleOnce(drain);
            }
            if (firstFailure.get() != null) {
                shutdownFailure();
            } else if (drain && coordinator.isDrainComplete()) {
                // The cached catalog alone never authorizes READY. This also
                // detects modifications of surviving known manifests made
                // after the first FINISHING reconciliation.
                coordinator.scanAndScheduleOnce(true);
                if (firstFailure.get() != null) {
                    shutdownFailure();
                } else if (coordinator.isDrainComplete()) {
                    completeSuccess();
                }
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
            recordWakeFailure(e);
        }
    }

    private void recordWakeFailure(final RejectedExecutionException cause) {
        if (!terminalized.get()) {
            firstFailure.compareAndSet(null, new IndexException(
                    "Unable to wake Senku coordinator.", cause));
        }
    }

    private void requireWriting(final String operation) {
        if (finishRequested || state != SenkuWritingState.WRITING) {
            final SenkuWritingState current = finishRequested
                    && state == SenkuWritingState.WRITING
                            ? SenkuWritingState.FINISHING
                            : state;
            throw new IndexException("Senku " + operation
                    + " is not allowed in state " + current + ".");
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
