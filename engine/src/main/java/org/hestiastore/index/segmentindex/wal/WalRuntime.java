package org.hestiastore.index.segmentindex.wal;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.configuration.api.IndexWalConfiguration;

/**
 * Active WAL runtime facade.
 *
 * <p>
 * The implementation is split across metadata/catalog, writer, recovery,
 * segment management, and sync-policy collaborators.
 *
 * <p>
 * Concurrency invariant: WAL state mutations run under {@link #monitor}, while
 * queue admission uses a fair read/write lock. Append producers share the read
 * side and therefore do not serialize with each other. Close takes the write
 * side before marking the runtime closed and enqueueing the stop marker, so every
 * append admitted before close remains ahead of that marker.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class WalRuntime<K, V> implements WalMonitoringView, AutoCloseable {

    /**
     * Directory name that stores WAL metadata and segment files under an index
     * directory.
     */
    public static final String WAL_DIRECTORY = WalMetadataCatalog.WAL_DIRECTORY;

    private static final String DEFAULT_THREAD_NAME_PREFIX = "hestia";
    private static final String DEFAULT_INDEX_NAME = "standalone";
    private static final String POOL_NAME_WAL_APPEND = "wal-append";
    private static final String ARG_THREAD_NAME_PREFIX = "threadNamePrefix";
    private static final String ARG_INDEX_NAME = "indexName";
    // ponytail: fixed internal queue; make configurable only if benchmarks need it.
    private static final int APPEND_QUEUE_CAPACITY = 8192;
    private static final int APPEND_DRAIN_LIMIT = 256;
    private static final long APPEND_OFFER_TIMEOUT_MILLIS = 10L;

    /**
     * WAL operation kind.
     */
    public enum Operation {
        PUT((byte) 1), DELETE((byte) 2);

        private final byte code;

        Operation(final byte code) {
            this.code = code;
        }

        byte code() {
            return code;
        }

        static Operation fromCode(final byte code) {
            return switch (code) {
                case 1 -> PUT;
                case 2 -> DELETE;
                default -> throw new IndexException("Invalid WAL operation code.");
            };
        }
    }

    /**
     * Parsed replay record from WAL.
     *
     * @param <K> key type
     * @param <V> value type
     */
    public static final class ReplayRecord<K, V> {
        private final long lsn;
        private final Operation operation;
        private final K key;
        private final V value;

        ReplayRecord(final long lsn, final Operation operation, final K key,
                final V value) {
            this.lsn = lsn;
            this.operation = operation;
            this.key = key;
            this.value = value;
        }

        public long getLsn() {
            return lsn;
        }

        public Operation getOperation() {
            return operation;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }
    }

    /**
     * Recovery summary.
     */
    public static final class RecoveryResult {
        private final long lastReplayedLsn;
        private final long maxLsn;
        private final boolean truncatedTail;

        /**
         * Creates a recovery summary.
         *
         * @param lastReplayedLsn last LSN replayed into the index
         * @param maxLsn          maximum valid WAL LSN seen during recovery
         * @param truncatedTail   whether recovery truncated an invalid WAL tail
         */
        public RecoveryResult(final long lastReplayedLsn, final long maxLsn,
                final boolean truncatedTail) {
            this.lastReplayedLsn = lastReplayedLsn;
            this.maxLsn = maxLsn;
            this.truncatedTail = truncatedTail;
        }

        /**
         * Returns the last LSN replayed into the index.
         *
         * @return last replayed LSN
         */
        public long lastReplayedLsn() {
            return lastReplayedLsn;
        }

        /**
         * Returns the maximum valid WAL LSN seen during recovery.
         *
         * @return maximum WAL LSN
         */
        public long maxLsn() {
            return maxLsn;
        }

        /**
         * Returns whether recovery truncated an invalid WAL tail.
         *
         * @return true when WAL tail was truncated
         */
        public boolean truncatedTail() {
            return truncatedTail;
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof RecoveryResult other)) {
                return false;
            }
            return lastReplayedLsn == other.lastReplayedLsn
                    && maxLsn == other.maxLsn
                    && truncatedTail == other.truncatedTail;
        }

        @Override
        public int hashCode() {
            return Objects.hash(Long.valueOf(lastReplayedLsn),
                    Long.valueOf(maxLsn), Boolean.valueOf(truncatedTail));
        }

        @Override
        public String toString() {
            return "RecoveryResult[lastReplayedLsn=" + lastReplayedLsn
                    + ", maxLsn=" + maxLsn + ", truncatedTail="
                    + truncatedTail + "]";
        }
    }

    private final Object monitor;
    private final WalRuntimeMetrics metrics;
    private final AtomicBoolean closed;
    private final WalStorage storage;
    private final WalMetadataCatalog metadataCatalog;
    private final WalSegmentCatalog segmentCatalog;
    private final WalSyncPolicy syncPolicy;
    private final WalWriter<K, V> writer;
    private final WalRecoveryManager<K, V> recoveryManager;
    private final BlockingQueue<WalAppendTask<K, V>> appendQueue;
    private final ReentrantReadWriteLock appendAdmission =
            new ReentrantReadWriteLock(true);
    private final Thread appendWorker;

    private long checkpointLsn = 0L;

    /**
     * Creates a WAL runtime and starts its single append worker.
     *
     * @param monitor WAL state monitor
     * @param metrics mutable WAL metrics
     * @param closed runtime closed flag
     * @param storage WAL storage
     * @param metadataCatalog WAL metadata catalog
     * @param segmentCatalog WAL segment catalog
     * @param syncPolicy WAL durability policy
     * @param writer ordered WAL writer
     * @param recoveryManager WAL recovery manager
     * @param appendQueue bounded append queue
     * @param appendThreadFactory factory producing one named daemon worker
     */
    @SuppressWarnings("java:S107")
    WalRuntime(final Object monitor,
            final WalRuntimeMetrics metrics, final AtomicBoolean closed,
            final WalStorage storage,
            final WalMetadataCatalog metadataCatalog,
            final WalSegmentCatalog segmentCatalog,
            final WalSyncPolicy syncPolicy, final WalWriter<K, V> writer,
            final WalRecoveryManager<K, V> recoveryManager,
            final BlockingQueue<WalAppendTask<K, V>> appendQueue,
            final ThreadFactory appendThreadFactory) {
        this.monitor = Vldtn.requireNonNull(monitor, "monitor");
        this.metrics = Vldtn.requireNonNull(metrics, "metrics");
        this.closed = Vldtn.requireNonNull(closed, "closed");
        this.storage = Vldtn.requireNonNull(storage, "storage");
        this.metadataCatalog = metadataCatalog;
        this.segmentCatalog = segmentCatalog;
        this.syncPolicy = syncPolicy;
        this.writer = writer;
        this.recoveryManager = recoveryManager;
        this.appendQueue = Vldtn.requireNonNull(appendQueue, "appendQueue");
        final ThreadFactory resolvedThreadFactory = Vldtn.requireNonNull(
                appendThreadFactory, "appendThreadFactory");
        this.appendWorker = Vldtn.requireNonNull(
                resolvedThreadFactory.newThread(this::runAppendWorker),
                "appendWorker");
        Vldtn.requireTrue(appendWorker.isDaemon(),
                "WAL append worker thread must be daemon.");
        appendWorker.start();
    }

    /**
     * Creates WAL runtime for the given index directory.
     *
     * @param <K>             key type
     * @param <V>             value type
     * @param indexDirectory  index directory
     * @param wal             WAL config
     * @param keyDescriptor   key descriptor
     * @param valueDescriptor value descriptor
     * @return runtime instance
     */
    public static <K, V> WalRuntime<K, V> open(final Directory indexDirectory,
            final IndexWalConfiguration wal,
            final TypeDescriptor<K> keyDescriptor,
            final TypeDescriptor<V> valueDescriptor) {
        return open(indexDirectory, wal, keyDescriptor, valueDescriptor,
                DEFAULT_THREAD_NAME_PREFIX, DEFAULT_INDEX_NAME);
    }

    /**
     * Creates WAL runtime for the given index directory and index-owned thread
     * name scope.
     *
     * @param <K>              key type
     * @param <V>              value type
     * @param indexDirectory   index directory
     * @param wal              WAL config
     * @param keyDescriptor    key descriptor
     * @param valueDescriptor  value descriptor
     * @param threadNamePrefix runtime-wide thread-name prefix
     * @param indexName        index name used in index-owned thread names
     * @return runtime instance
     */
    public static <K, V> WalRuntime<K, V> open(final Directory indexDirectory,
            final IndexWalConfiguration wal,
            final TypeDescriptor<K> keyDescriptor,
            final TypeDescriptor<V> valueDescriptor,
            final String threadNamePrefix,
            final String indexName) {
        final String resolvedThreadNamePrefix = Vldtn.requireNotBlank(
                threadNamePrefix, ARG_THREAD_NAME_PREFIX);
        final String resolvedIndexName = Vldtn.requireNotBlank(indexName,
                ARG_INDEX_NAME);
        final String appendThreadNamePrefix = poolThreadNamePrefix(
                resolvedThreadNamePrefix, resolvedIndexName,
                POOL_NAME_WAL_APPEND);
        return openWithAppendThreadFactory(indexDirectory, wal,
                keyDescriptor, valueDescriptor,
                standaloneAppendThreadFactory(appendThreadNamePrefix));
    }

    /**
     * Creates WAL runtime using an externally supplied append-worker factory.
     *
     * @param <K> key type
     * @param <V> value type
     * @param indexDirectory index directory
     * @param wal WAL config
     * @param keyDescriptor key descriptor
     * @param valueDescriptor value descriptor
     * @param appendThreadFactory factory producing one named daemon worker
     * @return runtime instance
     */
    public static <K, V> WalRuntime<K, V> open(
            final Directory indexDirectory,
            final IndexWalConfiguration wal,
            final TypeDescriptor<K> keyDescriptor,
            final TypeDescriptor<V> valueDescriptor,
            final ThreadFactory appendThreadFactory) {
        return openWithAppendThreadFactory(indexDirectory, wal, keyDescriptor,
                valueDescriptor, Vldtn.requireNonNull(appendThreadFactory,
                        "appendThreadFactory"));
    }

    private static <K, V> WalRuntime<K, V> openWithAppendThreadFactory(
            final Directory indexDirectory,
            final IndexWalConfiguration wal,
            final TypeDescriptor<K> keyDescriptor,
            final TypeDescriptor<V> valueDescriptor,
            final ThreadFactory appendThreadFactory) {
        final Directory directory = Vldtn.requireNonNull(indexDirectory,
                "indexDirectory");
        final IndexWalConfiguration resolvedWal = requireEnabledWal(wal);
        final Directory walDirectory = directory.openSubDirectory(WAL_DIRECTORY);
        final WalRuntime<K, V> runtime = createRuntime(resolvedWal,
                WalStorageFactory.create(walDirectory), keyDescriptor,
                valueDescriptor, appendThreadFactory);
        runtime.metadataCatalog.ensureFormatMarker();
        return runtime;
    }

    private static IndexWalConfiguration requireEnabledWal(
            final IndexWalConfiguration wal) {
        final IndexWalConfiguration resolvedWal = IndexWalConfiguration
                .orEmpty(wal);
        Vldtn.requireTrue(resolvedWal.isEnabled(),
                "WAL configuration must be enabled to create WalRuntime.");
        return resolvedWal;
    }

    private static <K, V> WalRuntime<K, V> createRuntime(
            final IndexWalConfiguration wal,
            final WalStorage storage, final TypeDescriptor<K> keyDescriptor,
            final TypeDescriptor<V> valueDescriptor,
            final ThreadFactory appendThreadFactory) {
        final Object monitor = new Object();
        final WalRuntimeMetrics metrics = new WalRuntimeMetrics();
        final AtomicBoolean closed = new AtomicBoolean();
        final WalMetadataCatalog metadataCatalog = new WalMetadataCatalog(
                storage);
        final WalRecordCodec<K, V> recordCodec = new WalRecordCodec<>(
                keyDescriptor == null ? null : keyDescriptor.getTypeEncoder(),
                keyDescriptor == null ? null : keyDescriptor.getTypeDecoder(),
                valueDescriptor == null ? null
                        : valueDescriptor.getTypeEncoder(),
                valueDescriptor == null ? null
                        : valueDescriptor.getTypeDecoder());
        final WalSegmentCatalog segmentCatalog = new WalSegmentCatalog(wal,
                storage, metadataCatalog);
        final WalSyncPolicy syncPolicy = new WalSyncPolicy(wal, storage,
                metrics, monitor, segmentCatalog);
        final WalWriter<K, V> writer = new WalWriter<>(storage,
                recordCodec, segmentCatalog, metrics);
        final WalRecoveryManager<K, V> recoveryManager = new WalRecoveryManager<>(wal, storage, metadataCatalog,
                recordCodec, segmentCatalog, metrics);
        return new WalRuntime<>(monitor, metrics, closed, storage, metadataCatalog,
                segmentCatalog, syncPolicy, writer, recoveryManager,
                new ArrayBlockingQueue<>(APPEND_QUEUE_CAPACITY),
                appendThreadFactory);
    }

    private static String poolThreadNamePrefix(final String prefix,
            final String indexName,
            final String poolName) {
        return prefix + "-" + indexName + "-" + poolName;
    }

    private static ThreadFactory standaloneAppendThreadFactory(
            final String threadNamePrefix) {
        final ThreadFactory defaultFactory = Executors.defaultThreadFactory();
        return runnable -> {
            final Thread thread = defaultFactory.newThread(runnable);
            thread.setName(threadNamePrefix + "-1");
            thread.setDaemon(true);
            return thread;
        };
    }

    /**
     * Replays WAL records above checkpoint and repairs invalid tail according
     * to configured policy.
     *
     * @param replayConsumer replay callback
     * @return recovery summary
     */
    public RecoveryResult recover(
            final Consumer<ReplayRecord<K, V>> replayConsumer) {
        Vldtn.requireNonNull(replayConsumer, "replayConsumer");
        synchronized (monitor) {
            syncPolicy.checkSyncFailure();
            ensureOpen();
            final WalRecoveryOutcome outcome = recoveryManager
                    .recover(replayConsumer);
            checkpointLsn = outcome.checkpointLsn();
            syncPolicy.resetAfterRecovery(outcome.maxLsn());
            writer.resetNextLsn(Math.max(1L, outcome.maxLsn() + 1L));
            return new RecoveryResult(outcome.lastReplayedLsn(),
                    outcome.maxLsn(), outcome.truncatedTail());
        }
    }

    /**
     * Appends a PUT record and returns assigned LSN.
     *
     * @param key   key
     * @param value value
     * @return assigned LSN
     */
    public long appendPut(final K key, final V value) {
        return append(Operation.PUT, key, value);
    }

    /**
     * Appends a DELETE record and returns assigned LSN.
     *
     * @param key key
     * @return assigned LSN
     */
    public long appendDelete(final K key) {
        return append(Operation.DELETE, key, null);
    }

    /**
     * Updates checkpoint LSN and deletes eligible sealed segments.
     *
     * @param checkpointLsn checkpoint LSN fully reflected in stable state
     */
    public void onCheckpoint(final long checkpointLsn) {
        synchronized (monitor) {
            syncPolicy.checkSyncFailure();
            ensureOpen();
            final long effectiveCheckpoint = Math.max(this.checkpointLsn,
                    checkpointLsn);
            if (effectiveCheckpoint > this.checkpointLsn) {
                this.checkpointLsn = effectiveCheckpoint;
                metadataCatalog.writeCheckpointLsnAtomic(this.checkpointLsn);
            }
            segmentCatalog.cleanupEligibleSegments(this.checkpointLsn);
        }
    }

    public boolean isRetentionPressure() {
        synchronized (monitor) {
            return segmentCatalog.isRetentionPressure();
        }
    }

    public long retainedBytes() {
        synchronized (monitor) {
            return segmentCatalog.retainedBytes();
        }
    }

    public long durableLsn() {
        return syncPolicy.durableLsn();
    }

    /**
     * Returns whether WAL synchronization has failed.
     *
     * @return true when a synchronization failure was recorded
     */
    public boolean hasSyncFailure() {
        return syncPolicy.hasSyncFailure();
    }

    /**
     * Returns the current immutable WAL monitoring snapshot.
     *
     * @return WAL monitoring snapshot
     */
    @Override
    public WalMonitoring statsSnapshot() {
        synchronized (monitor) {
            return metrics.snapshot(segmentCatalog.retainedBytes(),
                    segmentCatalog.segmentCount(), syncPolicy.durableLsn(),
                    checkpointLsn, syncPolicy.pendingSyncBytes());
        }
    }

    @Override
    public void close() {
        final Lock closeAdmission = appendAdmission.writeLock();
        closeAdmission.lock();
        try {
            synchronized (monitor) {
                if (closed.get()) {
                    return;
                }
                closed.set(true);
                monitor.notifyAll();
            }
            enqueueStopTask();
        } finally {
            closeAdmission.unlock();
        }
        joinAppendWorker();
        synchronized (monitor) {
            syncPolicy.closeAndFlushPending();
        }
        storage.close();
    }

    private long append(final Operation operation, final K key, final V value) {
        final WalAppendTask<K, V> task = new WalAppendTask<>(operation, key,
                value);
        enqueueAppend(task);
        final long lsn = awaitWrittenLsn(task);
        syncPolicy.waitUntilDurable(lsn);
        return lsn;
    }

    private void enqueueAppend(final WalAppendTask<K, V> task) {
        while (true) {
            final Lock admission = appendAdmission.readLock();
            try {
                admission.lockInterruptibly();
                try {
                    syncPolicy.checkSyncFailure();
                    ensureOpen();
                    if (appendQueue.offer(task, APPEND_OFFER_TIMEOUT_MILLIS,
                            TimeUnit.MILLISECONDS)) {
                        return;
                    }
                } finally {
                    admission.unlock();
                }
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IndexException(
                        "Interrupted while enqueueing WAL append.", ex);
            }
        }
    }

    private long awaitWrittenLsn(final WalAppendTask<K, V> task) {
        try {
            return task.writtenLsn().get().longValue();
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IndexException("Interrupted while waiting for WAL append.",
                    ex);
        } catch (final ExecutionException ex) {
            final Throwable cause = ex.getCause();
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IndexException("WAL append failed.", cause);
        }
    }

    private void runAppendWorker() {
        final List<WalAppendTask<K, V>> batch = new ArrayList<>(
                APPEND_DRAIN_LIMIT);
        boolean stop = false;
        while (!stop) {
            batch.clear();
            try {
                stop = takeAppendBatch(batch);
                appendBatch(batch);
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                failQueuedAppends(new IndexException(
                        "WAL append worker interrupted.", ex));
                return;
            }
        }
    }

    private boolean takeAppendBatch(final List<WalAppendTask<K, V>> batch)
            throws InterruptedException {
        final long groupSyncWaitNanos;
        synchronized (monitor) {
            groupSyncWaitNanos = syncPolicy
                    .pendingGroupSyncWaitNanosLocked();
        }
        final WalAppendTask<K, V> first = groupSyncWaitNanos > 0L
                ? appendQueue.poll(groupSyncWaitNanos, TimeUnit.NANOSECONDS)
                : appendQueue.take();
        if (first == null) {
            return false;
        }
        boolean stop = first.stop();
        if (!stop) {
            batch.add(first);
        }
        appendQueue.drainTo(batch, APPEND_DRAIN_LIMIT - batch.size());
        if (!batch.isEmpty() && batch.get(batch.size() - 1).stop()) {
            batch.remove(batch.size() - 1);
            stop = true;
        }
        return stop;
    }

    private void appendBatch(final List<WalAppendTask<K, V>> batch) {
        synchronized (monitor) {
            for (final WalAppendTask<K, V> task : batch) {
                appendTask(task);
            }
            completeAppendBatch(batch);
        }
    }

    private void appendTask(final WalAppendTask<K, V> task) {
        try {
            syncPolicy.checkSyncFailure();
            final WalAppendResult result = writer.append(task.operation(),
                    task.key(), task.value());
            syncPolicy.afterAppend(result.lsn(), result.recordBytes(),
                    result.segmentName());
            task.markWritten(result.lsn());
            if (!syncPolicy.completesAtAppendBatchBoundary()) {
                task.completeWritten();
            }
        } catch (final RuntimeException ex) {
            task.fail(ex);
        }
    }

    /**
     * Applies the batch sync boundary and completes only appends that can safely
     * advance to their configured durability wait.
     *
     * @param batch possibly empty append-worker batch in WAL order
     */
    private void completeAppendBatch(
            final List<WalAppendTask<K, V>> batch) {
        try {
            syncPolicy.afterAppendBatch();
            batch.forEach(WalAppendTask::completeWritten);
        } catch (final RuntimeException ex) {
            batch.forEach(task -> task.fail(ex));
        }
    }

    private void joinAppendWorker() {
        try {
            appendWorker.join();
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IndexException("Interrupted while stopping WAL append worker.",
                    ex);
        }
    }

    private void enqueueStopTask() {
        final WalAppendTask<K, V> stopTask = WalAppendTask.stopTask();
        while (true) {
            try {
                if (appendQueue.offer(stopTask, 10L, TimeUnit.MILLISECONDS)) {
                    return;
                }
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IndexException("Interrupted while stopping WAL append worker.",
                        ex);
            }
        }
    }

    private void failQueuedAppends(final RuntimeException failure) {
        WalAppendTask<K, V> task = appendQueue.poll();
        while (task != null) {
            if (!task.stop()) {
                task.fail(failure);
            }
            task = appendQueue.poll();
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IndexException("WAL runtime is already closed.");
        }
    }

}
