package org.hestiastore.index.segmentindex.wal;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.Wal;
import org.hestiastore.index.segmentindex.WalDurabilityMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compatibility-facing WAL runtime facade.
 *
 * <p>
 * Public behavior stays stable while the implementation is split across
 * metadata/catalog, writer, recovery, segment management, and sync-policy
 * collaborators.
 *
 * <p>
 * Concurrency invariant: all stateful operations other than best-effort
 * durability reads run under {@link #monitor}. Collaborators assume the caller
 * already owns that monitor for their `...Locked()` methods.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class WalRuntime<K, V> implements AutoCloseable {

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
            default -> null;
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
    public record RecoveryResult(long lastReplayedLsn, long maxLsn,
            boolean truncatedTail) {
    }

    /**
     * Replay callback.
     */
    @FunctionalInterface
    public interface ReplayConsumer<K, V> {
        void accept(ReplayRecord<K, V> replayRecord);
    }

    private static final Logger logger = LoggerFactory.getLogger(WalRuntime.class);

    private final boolean enabled;
    private final Wal wal;
    private final Object monitor = new Object();
    private final WalRuntimeMetrics metrics = new WalRuntimeMetrics();
    private final WalStorage storage;
    private final WalMetadataCatalog metadataCatalog;
    private final WalRecordCodec<K, V> recordCodec;
    private final WalSegmentCatalog segmentCatalog;
    private final WalSyncPolicy syncPolicy;
    private final WalWriter<K, V> writer;
    private final WalRecoveryManager<K, V> recoveryManager;
    private final ScheduledExecutorService groupSyncExecutor;

    private long checkpointLsn = 0L;
    private boolean closed;

    private WalRuntime(final Wal wal, final WalStorage storage,
            final TypeDescriptor<K> keyDescriptor,
            final TypeDescriptor<V> valueDescriptor) {
        this.wal = Wal.orEmpty(wal);
        this.enabled = this.wal.isEnabled();
        this.storage = storage;
        if (!enabled) {
            this.metadataCatalog = null;
            this.recordCodec = null;
            this.segmentCatalog = null;
            this.syncPolicy = null;
            this.writer = null;
            this.recoveryManager = null;
            this.groupSyncExecutor = null;
            return;
        }
        this.metadataCatalog = new WalMetadataCatalog(
                Vldtn.requireNonNull(storage, "storage"), logger);
        this.recordCodec = new WalRecordCodec<>(
                keyDescriptor == null ? null : keyDescriptor.getTypeEncoder(),
                keyDescriptor == null ? null : keyDescriptor.getTypeDecoder(),
                valueDescriptor == null ? null
                        : valueDescriptor.getTypeEncoder(),
                valueDescriptor == null ? null
                        : valueDescriptor.getTypeDecoder());
        this.segmentCatalog = new WalSegmentCatalog(this.wal, this.storage,
                this.metadataCatalog, logger);
        this.syncPolicy = new WalSyncPolicy(this.wal, this.storage, metrics,
                logger, monitor, segmentCatalog::segments, this::isClosed);
        this.writer = new WalWriter<>(this.wal, this.storage, this.recordCodec,
                this.segmentCatalog, metrics, this.syncPolicy);
        this.recoveryManager = new WalRecoveryManager<>(this.wal, this.storage,
                this.metadataCatalog, this.recordCodec, this.segmentCatalog,
                this.metrics, logger);
        if (this.wal.getDurabilityMode() == WalDurabilityMode.GROUP_SYNC
                && this.wal.getGroupSyncDelayMillis() > 0) {
            this.groupSyncExecutor = Executors.newSingleThreadScheduledExecutor(
                    new NamedDaemonThreadFactory("hestiastore-wal-group-sync"));
            this.groupSyncExecutor.scheduleWithFixedDelay(
                    syncPolicy::syncGroupPendingSafely,
                    this.wal.getGroupSyncDelayMillis(),
                    this.wal.getGroupSyncDelayMillis(), TimeUnit.MILLISECONDS);
        } else {
            this.groupSyncExecutor = null;
        }
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
            final Wal wal, final TypeDescriptor<K> keyDescriptor,
            final TypeDescriptor<V> valueDescriptor) {
        Vldtn.requireNonNull(indexDirectory, "indexDirectory");
        final Wal resolvedWal = Wal.orEmpty(wal);
        if (!resolvedWal.isEnabled()) {
            return new WalRuntime<>(resolvedWal, null, null, null);
        }
        final Directory walDirectory = indexDirectory
                .openSubDirectory(WalMetadataCatalog.WAL_DIRECTORY);
        final WalStorage storage = WalStorageFactory.create(walDirectory);
        final WalRuntime<K, V> runtime = new WalRuntime<>(resolvedWal, storage,
                keyDescriptor, valueDescriptor);
        runtime.metadataCatalog.ensureFormatMarker();
        return runtime;
    }

    static <K, V> WalRuntime<K, V> openForTests(final Wal wal,
            final WalStorage storage, final TypeDescriptor<K> keyDescriptor,
            final TypeDescriptor<V> valueDescriptor) {
        final WalRuntime<K, V> runtime = new WalRuntime<>(Wal.orEmpty(wal),
                Vldtn.requireNonNull(storage, "storage"), keyDescriptor,
                valueDescriptor);
        if (runtime.enabled) {
            runtime.metadataCatalog.ensureFormatMarker();
        }
        return runtime;
    }

    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Replays WAL records above checkpoint and repairs invalid tail according
     * to configured policy.
     *
     * @param replayConsumer replay callback
     * @return recovery summary
     */
    public RecoveryResult recover(final ReplayConsumer<K, V> replayConsumer) {
        if (!enabled) {
            return new RecoveryResult(0L, 0L, false);
        }
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
     * @param key key
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
        if (!enabled) {
            return;
        }
        synchronized (monitor) {
            syncPolicy.checkSyncFailure();
            ensureOpen();
            final long effectiveCheckpoint = Math.max(this.checkpointLsn,
                    checkpointLsn);
            if (effectiveCheckpoint == this.checkpointLsn) {
                segmentCatalog.cleanupEligibleSegments(this.checkpointLsn);
                return;
            }
            this.checkpointLsn = effectiveCheckpoint;
            metadataCatalog.writeCheckpointLsnAtomic(this.checkpointLsn);
            segmentCatalog.cleanupEligibleSegments(this.checkpointLsn);
        }
    }

    public boolean isRetentionPressure() {
        if (!enabled) {
            return false;
        }
        synchronized (monitor) {
            return segmentCatalog.isRetentionPressure();
        }
    }

    public long retainedBytes() {
        if (!enabled) {
            return 0L;
        }
        synchronized (monitor) {
            return segmentCatalog.retainedBytes();
        }
    }

    public long durableLsn() {
        if (!enabled) {
            return 0L;
        }
        return syncPolicy.durableLsn();
    }

    public boolean hasSyncFailure() {
        if (!enabled) {
            return false;
        }
        synchronized (monitor) {
            return syncPolicy.hasSyncFailure();
        }
    }

    public WalStats statsSnapshot() {
        if (!enabled) {
            return WalRuntimeMetrics.emptySnapshot();
        }
        synchronized (monitor) {
            return metrics.snapshot(segmentCatalog.retainedBytes(),
                    segmentCatalog.segmentCount(), syncPolicy.durableLsn(),
                    checkpointLsn, syncPolicy.pendingSyncBytes());
        }
    }

    @Override
    public void close() {
        if (!enabled) {
            return;
        }
        synchronized (monitor) {
            if (closed) {
                return;
            }
            closed = true;
            monitor.notifyAll();
        }
        if (groupSyncExecutor != null) {
            groupSyncExecutor.shutdownNow();
        }
        synchronized (monitor) {
            syncPolicy.closeAndFlushPending();
        }
    }

    private long append(final Operation operation, final K key, final V value) {
        if (!enabled) {
            return 0L;
        }
        synchronized (monitor) {
            syncPolicy.checkSyncFailure();
            ensureOpen();
            return writer.append(operation, key, value);
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IndexException("WAL runtime is already closed.");
        }
    }

    private boolean isClosed() {
        return closed;
    }

    static int computeCrc32(final byte[] data, final int offset,
            final int length) {
        return WalRecordCodec.computeCrc32(data, offset, length);
    }

    static void putInt(final byte[] bytes, final int offset, final int value) {
        WalRecordCodec.putInt(bytes, offset, value);
    }

    static int readInt(final byte[] bytes, final int offset) {
        return WalRecordCodec.readInt(bytes, offset);
    }

    static void putLong(final byte[] bytes, final int offset, final long value) {
        WalRecordCodec.putLong(bytes, offset, value);
    }

    static long readLong(final byte[] bytes, final int offset) {
        return WalRecordCodec.readLong(bytes, offset);
    }

    private static final class NamedDaemonThreadFactory
            implements ThreadFactory {

        private final String namePrefix;
        private final AtomicLong sequence = new AtomicLong(0L);

        NamedDaemonThreadFactory(final String namePrefix) {
            this.namePrefix = Vldtn.requireNonNull(namePrefix, "namePrefix");
        }

        @Override
        public Thread newThread(final Runnable runnable) {
            final Thread thread = new Thread(runnable,
                    namePrefix + "-" + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }
    }
}
