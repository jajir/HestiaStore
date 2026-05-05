package org.hestiastore.index.segmentindex.wal;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexWalConfiguration;
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

    private static final Logger logger = LoggerFactory
            .getLogger(WalRuntime.class);

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

    private final boolean enabled;
    private final Object monitor;
    private final WalRuntimeMetrics metrics;
    private final AtomicBoolean closed;
    private final WalMetadataCatalog metadataCatalog;
    private final WalSegmentCatalog segmentCatalog;
    private final WalSyncPolicy syncPolicy;
    private final WalWriter<K, V> writer;
    private final WalRecoveryManager<K, V> recoveryManager;
    private final ScheduledExecutorService groupSyncExecutor;

    private long checkpointLsn = 0L;

    @SuppressWarnings("java:S107")
    private WalRuntime(final IndexWalConfiguration wal, final Object monitor,
            final WalRuntimeMetrics metrics, final AtomicBoolean closed,
            final WalMetadataCatalog metadataCatalog,
            final WalSegmentCatalog segmentCatalog,
            final WalSyncPolicy syncPolicy, final WalWriter<K, V> writer,
            final WalRecoveryManager<K, V> recoveryManager,
            final ScheduledExecutorService groupSyncExecutor) {
        this.enabled = Vldtn.requireNonNull(wal, "wal").isEnabled();
        this.monitor = Vldtn.requireNonNull(monitor, "monitor");
        this.metrics = Vldtn.requireNonNull(metrics, "metrics");
        this.closed = Vldtn.requireNonNull(closed, "closed");
        this.metadataCatalog = metadataCatalog;
        this.segmentCatalog = segmentCatalog;
        this.syncPolicy = syncPolicy;
        this.writer = writer;
        this.recoveryManager = recoveryManager;
        this.groupSyncExecutor = groupSyncExecutor;
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
            final IndexWalConfiguration wal, final TypeDescriptor<K> keyDescriptor,
            final TypeDescriptor<V> valueDescriptor) {
        Vldtn.requireNonNull(indexDirectory, "indexDirectory");
        final IndexWalConfiguration resolvedWal = IndexWalConfiguration.orEmpty(wal);
        if (!resolvedWal.isEnabled()) {
            return disabled(resolvedWal);
        }
        final Directory walDirectory = indexDirectory
                .openSubDirectory(WalMetadataCatalog.WAL_DIRECTORY);
        final WalStorage storage = WalStorageFactory.create(walDirectory);
        final WalRuntime<K, V> runtime = create(resolvedWal, storage,
                keyDescriptor, valueDescriptor);
        runtime.metadataCatalog.ensureFormatMarker();
        return runtime;
    }

    static <K, V> WalRuntime<K, V> openForTests(final IndexWalConfiguration wal,
            final WalStorage storage, final TypeDescriptor<K> keyDescriptor,
            final TypeDescriptor<V> valueDescriptor) {
        final WalRuntime<K, V> runtime = create(IndexWalConfiguration.orEmpty(wal),
                Vldtn.requireNonNull(storage, "storage"), keyDescriptor,
                valueDescriptor);
        if (runtime.enabled) {
            runtime.metadataCatalog.ensureFormatMarker();
        }
        return runtime;
    }

    private static <K, V> WalRuntime<K, V> disabled(
            final IndexWalConfiguration wal) {
        return new WalRuntime<>(wal, new Object(), new WalRuntimeMetrics(),
                new AtomicBoolean(), null, null, null, null, null, null);
    }

    private static <K, V> WalRuntime<K, V> create(
            final IndexWalConfiguration wal, final WalStorage storage,
            final TypeDescriptor<K> keyDescriptor,
            final TypeDescriptor<V> valueDescriptor) {
        if (!wal.isEnabled()) {
            return disabled(wal);
        }
        final Object monitor = new Object();
        final WalRuntimeMetrics metrics = new WalRuntimeMetrics();
        final AtomicBoolean closed = new AtomicBoolean();
        final WalMetadataCatalog metadataCatalog = new WalMetadataCatalog(
                storage, logger);
        final WalRecordCodec<K, V> recordCodec = new WalRecordCodec<>(
                keyDescriptor == null ? null : keyDescriptor.getTypeEncoder(),
                keyDescriptor == null ? null : keyDescriptor.getTypeDecoder(),
                valueDescriptor == null ? null
                        : valueDescriptor.getTypeEncoder(),
                valueDescriptor == null ? null
                        : valueDescriptor.getTypeDecoder());
        final WalSegmentCatalog segmentCatalog = new WalSegmentCatalog(wal,
                storage, metadataCatalog, logger);
        final WalSyncPolicy syncPolicy = new WalSyncPolicy(wal, storage,
                metrics, logger, monitor, segmentCatalog::segments,
                closed::get);
        final WalWriter<K, V> writer = new WalWriter<>(wal, storage,
                recordCodec, segmentCatalog, metrics, syncPolicy);
        final WalRecoveryManager<K, V> recoveryManager =
                new WalRecoveryManager<>(wal, storage, metadataCatalog,
                        recordCodec, segmentCatalog, metrics, logger);
        return new WalRuntime<>(wal, monitor, metrics, closed, metadataCatalog,
                segmentCatalog, syncPolicy, writer, recoveryManager,
                newGroupSyncExecutor(wal, syncPolicy));
    }

    private static ScheduledExecutorService newGroupSyncExecutor(
            final IndexWalConfiguration wal, final WalSyncPolicy syncPolicy) {
        if (wal.getDurabilityMode() != WalDurabilityMode.GROUP_SYNC
                || wal.getGroupSyncDelayMillis() <= 0) {
            return null;
        }
        final ScheduledExecutorService executor =
                Executors.newSingleThreadScheduledExecutor(
                        new NamedDaemonThreadFactory(
                                "hestiastore-wal-group-sync"));
        executor.scheduleWithFixedDelay(syncPolicy::syncGroupPendingSafely,
                wal.getGroupSyncDelayMillis(), wal.getGroupSyncDelayMillis(),
                TimeUnit.MILLISECONDS);
        return executor;
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
            if (closed.get()) {
                return;
            }
            closed.set(true);
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
        if (closed.get()) {
            throw new IndexException("WAL runtime is already closed.");
        }
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
