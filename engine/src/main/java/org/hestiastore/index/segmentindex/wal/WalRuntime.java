package org.hestiastore.index.segmentindex.wal;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.zip.CRC32;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDecoder;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeEncoder;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.Wal;
import org.hestiastore.index.segmentindex.WalCorruptionPolicy;
import org.hestiastore.index.segmentindex.WalDurabilityMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WAL runtime that handles append, recovery, checkpoints, rotation and
 * retention cleanup.
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
        void accept(ReplayRecord<K, V> record);
    }

    private static final Logger logger = LoggerFactory.getLogger(WalRuntime.class);

    private static final String WAL_DIRECTORY = "wal";
    private static final String FORMAT_FILE = "format.meta";
    private static final String CHECKPOINT_FILE = "checkpoint.meta";
    private static final String CHECKPOINT_FILE_TMP = "checkpoint.meta.tmp";
    private static final String FORMAT_FILE_TMP = "format.meta.tmp";
    private static final String SEGMENT_SUFFIX = ".wal";
    private static final String SEGMENT_FILE_FORMAT = "%020d" + SEGMENT_SUFFIX;
    private static final int FORMAT_VERSION = 1;
    private static final int MIN_RECORD_BODY_SIZE = 4 + 8 + 1 + 4 + 4;
    private static final int MAX_RECORD_BODY_SIZE = 32 * 1024 * 1024;
    private static final int BUFFER_SIZE = 8 * 1024;

    private final boolean enabled;
    private final Wal wal;
    private final WalStorage storage;
    private final TypeEncoder<K> keyEncoder;
    private final TypeDecoder<K> keyDecoder;
    private final TypeEncoder<V> valueEncoder;
    private final TypeDecoder<V> valueDecoder;
    private final Object monitor = new Object();
    private final List<SegmentInfo> segments = new ArrayList<>();
    private final AtomicLong durableLsn = new AtomicLong(0L);
    private final ScheduledExecutorService groupSyncExecutor;

    private final LongAdder appendCount = new LongAdder();
    private final LongAdder appendBytes = new LongAdder();
    private final LongAdder syncCount = new LongAdder();
    private final LongAdder syncTotalNanos = new LongAdder();
    private final LongAdder syncBatchBytesTotal = new LongAdder();
    private final LongAdder syncFailureCount = new LongAdder();
    private final LongAdder corruptionCount = new LongAdder();
    private final LongAdder truncationCount = new LongAdder();
    private final AtomicLong syncMaxNanos = new AtomicLong(0L);
    private final AtomicLong syncBatchBytesMax = new AtomicLong(0L);

    private long nextLsn = 1L;
    private long checkpointLsn = 0L;
    private long retainedBytes = 0L;
    private long pendingSyncHighLsn = 0L;
    private long pendingSyncBytes = 0L;
    private final Set<String> pendingSyncSegmentNames = new LinkedHashSet<>();
    private RuntimeException syncFailure;
    private boolean closed;

    private WalRuntime(final Wal wal, final WalStorage storage,
            final TypeDescriptor<K> keyDescriptor,
            final TypeDescriptor<V> valueDescriptor) {
        this.wal = Wal.orEmpty(wal);
        this.enabled = this.wal.isEnabled();
        this.storage = storage;
        this.keyEncoder = keyDescriptor == null ? null
                : keyDescriptor.getTypeEncoder();
        this.keyDecoder = keyDescriptor == null ? null
                : keyDescriptor.getTypeDecoder();
        this.valueEncoder = valueDescriptor == null ? null
                : valueDescriptor.getTypeEncoder();
        this.valueDecoder = valueDescriptor == null ? null
                : valueDescriptor.getTypeDecoder();
        if (enabled && this.wal.getDurabilityMode() == WalDurabilityMode.GROUP_SYNC
                && this.wal.getGroupSyncDelayMillis() > 0) {
            this.groupSyncExecutor = Executors.newSingleThreadScheduledExecutor(
                    new NamedDaemonThreadFactory("hestiastore-wal-group-sync"));
            this.groupSyncExecutor.scheduleWithFixedDelay(
                    this::syncGroupPendingSafely,
                    this.wal.getGroupSyncDelayMillis(),
                    this.wal.getGroupSyncDelayMillis(), TimeUnit.MILLISECONDS);
        } else {
            this.groupSyncExecutor = null;
        }
    }

    /**
     * Creates WAL runtime for the given index directory.
     *
     * @param <K>            key type
     * @param <V>            value type
     * @param indexDirectory index directory
     * @param wal            WAL config
     * @param keyDescriptor  key descriptor
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
                .openSubDirectory(WAL_DIRECTORY);
        final WalStorage storage = WalStorageFactory.create(walDirectory);
        final WalRuntime<K, V> runtime = new WalRuntime<>(resolvedWal, storage,
                keyDescriptor, valueDescriptor);
        runtime.ensureFormatMarker();
        return runtime;
    }

    static <K, V> WalRuntime<K, V> openForTests(final Wal wal,
            final WalStorage storage, final TypeDescriptor<K> keyDescriptor,
            final TypeDescriptor<V> valueDescriptor) {
        final WalRuntime<K, V> runtime = new WalRuntime<>(Wal.orEmpty(wal),
                Vldtn.requireNonNull(storage, "storage"), keyDescriptor,
                valueDescriptor);
        if (runtime.enabled) {
            runtime.ensureFormatMarker();
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
            checkSyncFailure();
            ensureOpen();
            ensureFormatMarker();
            checkpointLsn = readCheckpointLsn();
            retainedBytes = 0L;
            pendingSyncBytes = 0L;
            pendingSyncSegmentNames.clear();
            segments.clear();
            final List<SegmentInfo> discoveredSegments = discoverSegmentsStrict();
            boolean truncatedTail = false;
            long maxLsn = checkpointLsn;
            long lastReplayedLsn = checkpointLsn;
            for (int i = 0; i < discoveredSegments.size(); i++) {
                final SegmentInfo current = discoveredSegments.get(i);
                final ScanResult scan = scanAndReplaySegment(current.name(),
                        checkpointLsn, replayConsumer);
                if (scan.maxLsn() > maxLsn) {
                    maxLsn = scan.maxLsn();
                }
                if (scan.lastReplayedLsn() > lastReplayedLsn) {
                    lastReplayedLsn = scan.lastReplayedLsn();
                }
                if (scan.invalidTail()) {
                    truncatedTail = true;
                    handleInvalidTail(current.name(), scan.validBytes());
                    deleteSegmentsAfter(discoveredSegments, i + 1);
                    if (scan.validBytes() > 0L) {
                        final long segmentMaxLsn = scan.maxLsn() > 0L
                                ? scan.maxLsn()
                                : current.baseLsn();
                        segments.add(new SegmentInfo(current.name(),
                                current.baseLsn(), scan.validBytes(),
                                segmentMaxLsn));
                        retainedBytes += scan.validBytes();
                    }
                    break;
                }
                if (scan.validBytes() > 0L) {
                    final long segmentMaxLsn = scan.maxLsn() > 0L
                            ? scan.maxLsn()
                            : current.baseLsn();
                    segments.add(new SegmentInfo(current.name(),
                            current.baseLsn(), scan.validBytes(),
                            segmentMaxLsn));
                    retainedBytes += scan.validBytes();
                } else {
                    storage.delete(current.name());
                }
            }
            if (checkpointLsn > maxLsn) {
                checkpointLsn = maxLsn;
                writeCheckpointLsnAtomic(checkpointLsn);
            }
            durableLsn.set(maxLsn);
            pendingSyncHighLsn = maxLsn;
            nextLsn = Math.max(1L, maxLsn + 1L);
            syncFailure = null;
            logger.info(
                    "WAL recovery finished: maxLsn={}, checkpointLsn={}, replayedLsn={}, truncatedTail={}, segments={}",
                    maxLsn, checkpointLsn, lastReplayedLsn, truncatedTail,
                    segments.size());
            return new RecoveryResult(lastReplayedLsn, maxLsn, truncatedTail);
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
            checkSyncFailure();
            ensureOpen();
            final long effectiveCheckpoint = Math.max(this.checkpointLsn,
                    checkpointLsn);
            if (effectiveCheckpoint == this.checkpointLsn) {
                cleanupEligibleSegments();
                return;
            }
            this.checkpointLsn = effectiveCheckpoint;
            writeCheckpointLsnAtomic(this.checkpointLsn);
            cleanupEligibleSegments();
        }
    }

    public boolean isRetentionPressure() {
        if (!enabled) {
            return false;
        }
        synchronized (monitor) {
            return retainedBytes > wal.getMaxBytesBeforeForcedCheckpoint();
        }
    }

    public long retainedBytes() {
        if (!enabled) {
            return 0L;
        }
        synchronized (monitor) {
            return retainedBytes;
        }
    }

    public long durableLsn() {
        return durableLsn.get();
    }

    public boolean hasSyncFailure() {
        if (!enabled) {
            return false;
        }
        synchronized (monitor) {
            return syncFailure != null;
        }
    }

    public WalStats statsSnapshot() {
        if (!enabled) {
            return new WalStats(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0L, 0L, 0L, 0L,
                    0L, 0L, 0L);
        }
        synchronized (monitor) {
            return new WalStats(appendCount.sum(), appendBytes.sum(),
                    syncCount.sum(), syncFailureCount.sum(),
                    corruptionCount.sum(), truncationCount.sum(), retainedBytes,
                    segments.size(), durableLsn.get(), checkpointLsn,
                    pendingSyncBytes, syncTotalNanos.sum(), syncMaxNanos.get(),
                    syncBatchBytesTotal.sum(), syncBatchBytesMax.get());
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
            if (wal.getDurabilityMode() != WalDurabilityMode.ASYNC
                    && syncFailure == null) {
                try {
                    syncGroupPendingLocked();
                } catch (RuntimeException ex) {
                    markSyncFailure(ex);
                }
            }
        }
    }

    private long append(final Operation operation, final K key, final V value) {
        if (!enabled) {
            return 0L;
        }
        Objects.requireNonNull(key, "key");
        if (operation == Operation.PUT) {
            Objects.requireNonNull(value, "value");
        }
        synchronized (monitor) {
            checkSyncFailure();
            ensureOpen();
            final long lsn = nextLsn;
            final byte[] recordBytes = encodeRecord(operation, lsn, key, value);
            ensureActiveSegmentFor(recordBytes.length);
            final SegmentInfo activeSegment = segments.get(segments.size() - 1);
            storage.append(activeSegment.name(), recordBytes, 0,
                    recordBytes.length);
            activeSegment.grow(recordBytes.length, lsn);
            retainedBytes += recordBytes.length;
            appendCount.increment();
            appendBytes.add(recordBytes.length);
            nextLsn = lsn + 1L;

            if (wal.getDurabilityMode() == WalDurabilityMode.ASYNC) {
                return lsn;
            }

            pendingSyncHighLsn = Math.max(pendingSyncHighLsn, lsn);
            pendingSyncBytes += recordBytes.length;
            pendingSyncSegmentNames.add(activeSegment.name());

            if (wal.getDurabilityMode() == WalDurabilityMode.SYNC
                    || wal.getGroupSyncDelayMillis() <= 0
                    || pendingSyncBytes >= wal.getGroupSyncMaxBatchBytes()) {
                syncGroupPendingLocked();
            }

            if (wal.getDurabilityMode() == WalDurabilityMode.GROUP_SYNC) {
                waitUntilDurableLocked(lsn);
            }
            return lsn;
        }
    }

    private void ensureActiveSegmentFor(final int nextRecordBytes) {
        if (segments.isEmpty()) {
            createSegment(nextLsn);
            return;
        }
        final SegmentInfo active = segments.get(segments.size() - 1);
        if (active.sizeBytes() + nextRecordBytes <= wal.getSegmentSizeBytes()) {
            return;
        }
        createSegment(nextLsn);
    }

    private void createSegment(final long baseLsn) {
        final String name = toSegmentFileName(baseLsn);
        storage.touch(name);
        segments.add(new SegmentInfo(name, baseLsn, 0L, baseLsn));
    }

    private byte[] encodeRecord(final Operation operation, final long lsn,
            final K key, final V value) {
        final byte[] keyBytes = encodeKey(key);
        final byte[] valueBytes = operation == Operation.PUT ? encodeValue(value)
                : new byte[0];
        final int bodyLen = MIN_RECORD_BODY_SIZE + keyBytes.length
                + valueBytes.length;
        if (bodyLen > MAX_RECORD_BODY_SIZE) {
            throw new IllegalArgumentException(String.format(
                    "WAL record body is too large: %s", bodyLen));
        }
        final byte[] body = new byte[bodyLen];
        int offset = 0;
        offset += 4; // CRC placeholder
        putLong(body, offset, lsn);
        offset += 8;
        body[offset++] = operation.code();
        putInt(body, offset, keyBytes.length);
        offset += 4;
        putInt(body, offset, valueBytes.length);
        offset += 4;
        System.arraycopy(keyBytes, 0, body, offset, keyBytes.length);
        offset += keyBytes.length;
        System.arraycopy(valueBytes, 0, body, offset, valueBytes.length);
        final int crc = computeCrc32(body, 4, body.length - 4);
        putInt(body, 0, crc);
        final byte[] encoded = new byte[4 + body.length];
        putInt(encoded, 0, body.length);
        System.arraycopy(body, 0, encoded, 4, body.length);
        return encoded;
    }

    private ScanResult scanAndReplaySegment(final String fileName,
            final long replayAfterLsn,
            final ReplayConsumer<K, V> replayConsumer) {
        long offset = 0L;
        long validOffset = 0L;
        long maxLsn = 0L;
        long lastReplayedLsn = replayAfterLsn;
        long previousLsn = 0L;
        while (true) {
            final byte[] lenBytes = new byte[4];
            final int lenRead = readFullyAllowEof(fileName, offset, lenBytes, 0,
                    4);
            if (lenRead == -1) {
                break;
            }
            if (lenRead != 4) {
                return new ScanResult(validOffset, maxLsn, lastReplayedLsn,
                        true);
            }
            final int bodyLen = readInt(lenBytes, 0);
            if (bodyLen < MIN_RECORD_BODY_SIZE || bodyLen > MAX_RECORD_BODY_SIZE) {
                return new ScanResult(validOffset, maxLsn, lastReplayedLsn,
                        true);
            }
            final byte[] body = new byte[bodyLen];
            if (!readFully(fileName, offset + 4L, body, 0, bodyLen)) {
                return new ScanResult(validOffset, maxLsn, lastReplayedLsn,
                        true);
            }
            final int storedCrc = readInt(body, 0);
            final int computedCrc = computeCrc32(body, 4, bodyLen - 4);
            if (storedCrc != computedCrc) {
                return new ScanResult(validOffset, maxLsn, lastReplayedLsn,
                        true);
            }
            int position = 4;
            final long lsn = readLong(body, position);
            position += 8;
            final Operation operation = Operation.fromCode(body[position++]);
            if (operation == null) {
                return new ScanResult(validOffset, maxLsn, lastReplayedLsn,
                        true);
            }
            final int keyLen = readInt(body, position);
            position += 4;
            final int valueLen = readInt(body, position);
            position += 4;
            if (keyLen <= 0 || valueLen < 0
                    || position + keyLen + valueLen != body.length) {
                return new ScanResult(validOffset, maxLsn, lastReplayedLsn,
                        true);
            }
            if (lsn <= 0L || (previousLsn > 0L && lsn <= previousLsn)) {
                return new ScanResult(validOffset, maxLsn, lastReplayedLsn,
                        true);
            }
            final byte[] keyBytes = new byte[keyLen];
            System.arraycopy(body, position, keyBytes, 0, keyLen);
            position += keyLen;
            final byte[] valueBytes = new byte[valueLen];
            if (valueLen > 0) {
                System.arraycopy(body, position, valueBytes, 0, valueLen);
            }
            final K key = decodeKey(keyBytes);
            final V value = operation == Operation.PUT ? decodeValue(valueBytes)
                    : null;
            if (operation == Operation.DELETE && valueLen != 0) {
                return new ScanResult(validOffset, maxLsn, lastReplayedLsn,
                        true);
            }
            offset += 4L + bodyLen;
            validOffset = offset;
            previousLsn = lsn;
            if (lsn > maxLsn) {
                maxLsn = lsn;
            }
            if (lsn > replayAfterLsn) {
                replayConsumer.accept(new ReplayRecord<>(lsn, operation, key,
                        value));
                if (lsn > lastReplayedLsn) {
                    lastReplayedLsn = lsn;
                }
            }
        }
        return new ScanResult(validOffset, maxLsn, lastReplayedLsn, false);
    }

    private void handleInvalidTail(final String fileName, final long validBytes) {
        corruptionCount.increment();
        if (wal.getCorruptionPolicy() == WalCorruptionPolicy.FAIL_FAST) {
            throw new IndexException(
                    String.format("WAL corruption detected in '%s'.", fileName));
        }
        truncationCount.increment();
        if (validBytes <= 0L) {
            storage.delete(fileName);
            logger.warn("WAL tail corruption: deleted file '{}'.", fileName);
            return;
        }
        storage.truncate(fileName, validBytes);
        storage.sync(fileName);
        logger.warn("WAL tail corruption: truncated '{}' to {} bytes.", fileName,
                validBytes);
    }

    private void deleteSegmentsAfter(final List<SegmentInfo> discoveredSegments,
            final int startIndex) {
        for (int i = startIndex; i < discoveredSegments.size(); i++) {
            storage.delete(discoveredSegments.get(i).name());
        }
    }

    private void cleanupEligibleSegments() {
        if (segments.size() <= 1) {
            return;
        }
        final List<SegmentInfo> retained = new ArrayList<>(segments.size());
        for (int i = 0; i < segments.size(); i++) {
            final SegmentInfo segment = segments.get(i);
            final boolean active = i == segments.size() - 1;
            if (!active && segment.maxLsn() <= checkpointLsn) {
                storage.delete(segment.name());
                retainedBytes -= segment.sizeBytes();
            } else {
                retained.add(segment);
            }
        }
        segments.clear();
        segments.addAll(retained);
        if (retainedBytes < 0L) {
            retainedBytes = 0L;
        }
    }

    private void syncGroupPendingSafely() {
        if (!enabled) {
            return;
        }
        synchronized (monitor) {
            if (closed) {
                return;
            }
            try {
                syncGroupPendingLocked();
            } catch (RuntimeException ex) {
                markSyncFailure(ex);
            }
        }
    }

    private void syncGroupPendingLocked() {
        if (!enabled || wal.getDurabilityMode() == WalDurabilityMode.ASYNC) {
            return;
        }
        checkSyncFailure();
        if (pendingSyncHighLsn <= durableLsn.get()) {
            pendingSyncBytes = 0L;
            pendingSyncSegmentNames.clear();
            return;
        }
        if (pendingSyncSegmentNames.isEmpty()) {
            return;
        }
        final long batchBytes = pendingSyncBytes;
        final long startedNanos = System.nanoTime();
        try {
            final Set<String> remaining = new HashSet<>(pendingSyncSegmentNames);
            for (final SegmentInfo segment : segments) {
                if (remaining.remove(segment.name())) {
                    storage.sync(segment.name());
                }
            }
            for (final String segmentName : remaining) {
                storage.sync(segmentName);
            }
            durableLsn.set(pendingSyncHighLsn);
            pendingSyncBytes = 0L;
            pendingSyncSegmentNames.clear();
            syncCount.increment();
            final long elapsedNanos = Math.max(0L,
                    System.nanoTime() - startedNanos);
            syncTotalNanos.add(elapsedNanos);
            syncBatchBytesTotal.add(Math.max(0L, batchBytes));
            updateMax(syncMaxNanos, elapsedNanos);
            updateMax(syncBatchBytesMax, Math.max(0L, batchBytes));
            monitor.notifyAll();
        } catch (RuntimeException ex) {
            markSyncFailure(ex);
            checkSyncFailure();
        }
    }

    private void waitUntilDurableLocked(final long lsn) {
        while (durableLsn.get() < lsn) {
            checkSyncFailure();
            if (closed) {
                throw new IndexException(
                        "WAL runtime closed while waiting for durability.");
            }
            try {
                monitor.wait(Math.max(1L, wal.getGroupSyncDelayMillis()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IndexException(
                        "Interrupted while waiting for group WAL sync.", e);
            }
        }
    }

    private void markSyncFailure(final RuntimeException ex) {
        if (syncFailure == null) {
            syncFailure = ex;
        }
        syncFailureCount.increment();
        logger.error("WAL sync failure", ex);
        monitor.notifyAll();
    }

    private void checkSyncFailure() {
        if (syncFailure != null) {
            throw new IndexException("WAL sync failure", syncFailure);
        }
    }

    private static void updateMax(final AtomicLong target, final long candidate) {
        while (true) {
            final long current = target.get();
            if (candidate <= current) {
                return;
            }
            if (target.compareAndSet(current, candidate)) {
                return;
            }
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IndexException("WAL runtime is already closed.");
        }
    }

    private List<SegmentInfo> discoverSegmentsStrict() {
        final List<String> segmentNames;
        try (StreamCloseable names = new StreamCloseable(storage.listFileNames())) {
            segmentNames = names.stream().filter(name -> name.endsWith(SEGMENT_SUFFIX))
                    .sorted().toList();
        }
        final List<SegmentInfo> discovered = new ArrayList<>(segmentNames.size());
        final Set<Long> uniqueBaseLsns = new HashSet<>();
        for (final String name : segmentNames) {
            final long baseLsn = parseSegmentBaseLsn(name);
            if (baseLsn < 0L) {
                throw new IndexException(String.format(
                        "Invalid WAL segment name '%s'.", name));
            }
            if (!uniqueBaseLsns.add(baseLsn)) {
                throw new IndexException(String.format(
                        "Duplicate WAL segment base LSN detected for '%s'.",
                        baseLsn));
            }
            discovered.add(new SegmentInfo(name, baseLsn, 0L, baseLsn));
        }
        discovered.sort(Comparator.comparingLong(SegmentInfo::baseLsn));
        return discovered;
    }

    private long parseSegmentBaseLsn(final String fileName) {
        final String raw = fileName.substring(0,
                fileName.length() - SEGMENT_SUFFIX.length());
        if (raw.isBlank()) {
            return -1L;
        }
        for (int i = 0; i < raw.length(); i++) {
            if (!Character.isDigit(raw.charAt(i))) {
                return -1L;
            }
        }
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException ex) {
            return -1L;
        }
    }

    private String toSegmentFileName(final long baseLsn) {
        return String.format(Locale.ROOT, SEGMENT_FILE_FORMAT, baseLsn);
    }

    private long readCheckpointLsn() {
        if (!storage.exists(CHECKPOINT_FILE)) {
            return 0L;
        }
        final byte[] data = storage.readAll(CHECKPOINT_FILE);
        if (data.length == 0) {
            return 0L;
        }
        try {
            final String value = new String(data, StandardCharsets.US_ASCII)
                    .trim();
            if (value.isEmpty()) {
                return 0L;
            }
            final long parsed = Long.parseLong(value);
            return Math.max(0L, parsed);
        } catch (RuntimeException ex) {
            return 0L;
        }
    }

    private void writeCheckpointLsnAtomic(final long checkpointLsn) {
        final byte[] data = String.valueOf(checkpointLsn)
                .getBytes(StandardCharsets.US_ASCII);
        storage.overwrite(CHECKPOINT_FILE_TMP, data, 0, data.length);
        storage.sync(CHECKPOINT_FILE_TMP);
        storage.rename(CHECKPOINT_FILE_TMP, CHECKPOINT_FILE);
        storage.sync(CHECKPOINT_FILE);
    }

    private void ensureFormatMarker() {
        if (!enabled) {
            return;
        }
        final byte[] payload = formatPayload();
        if (!storage.exists(FORMAT_FILE)) {
            writeFormatMarker(payload);
            return;
        }
        final byte[] existing = storage.readAll(FORMAT_FILE);
        final FormatMeta meta = parseFormatMeta(existing);
        final int expectedChecksum = computeCrc32(payload, 0, payload.length);
        if (meta.version() != FORMAT_VERSION || meta.checksum() != expectedChecksum) {
            throw new IndexException(String.format(
                    "Unsupported or corrupted WAL format metadata: version=%s checksum=%s expectedVersion=%s expectedChecksum=%s",
                    meta.version(), meta.checksum(), FORMAT_VERSION,
                    expectedChecksum));
        }
    }

    private void writeFormatMarker(final byte[] payload) {
        final int checksum = computeCrc32(payload, 0, payload.length);
        final byte[] data = ("version=" + FORMAT_VERSION + "\nchecksum="
                + checksum + "\n").getBytes(StandardCharsets.US_ASCII);
        storage.overwrite(FORMAT_FILE_TMP, data, 0, data.length);
        storage.sync(FORMAT_FILE_TMP);
        storage.rename(FORMAT_FILE_TMP, FORMAT_FILE);
        storage.sync(FORMAT_FILE);
    }

    private byte[] formatPayload() {
        return ("version=" + FORMAT_VERSION + "\n")
                .getBytes(StandardCharsets.US_ASCII);
    }

    private FormatMeta parseFormatMeta(final byte[] bytes) {
        final String text = new String(bytes, StandardCharsets.US_ASCII).trim();
        if (text.isEmpty()) {
            throw new IndexException("WAL format metadata is empty.");
        }
        Integer version = null;
        Integer checksum = null;
        final String[] lines = text.split("\\R");
        for (final String line : lines) {
            final String[] parts = line.split("=", 2);
            if (parts.length != 2) {
                continue;
            }
            final String key = parts[0].trim();
            final String value = parts[1].trim();
            if ("version".equals(key)) {
                version = Integer.valueOf(value);
            } else if ("checksum".equals(key)) {
                checksum = Integer.valueOf(value);
            }
        }
        if (version == null || checksum == null) {
            throw new IndexException("Invalid WAL format metadata.");
        }
        return new FormatMeta(version.intValue(), checksum.intValue());
    }

    private int readFullyAllowEof(final String fileName, final long position,
            final byte[] destination, final int offset, final int length) {
        int totalRead = 0;
        long currentPosition = position;
        while (totalRead < length) {
            final int read = storage.read(fileName, currentPosition, destination,
                    offset + totalRead, length - totalRead);
            if (read < 0) {
                return totalRead == 0 ? -1 : totalRead;
            }
            if (read == 0) {
                continue;
            }
            totalRead += read;
            currentPosition += read;
        }
        return totalRead;
    }

    private boolean readFully(final String fileName, final long position,
            final byte[] destination, final int offset, final int length) {
        int totalRead = 0;
        long currentPosition = position;
        while (totalRead < length) {
            final int read = storage.read(fileName, currentPosition, destination,
                    offset + totalRead, length - totalRead);
            if (read < 0) {
                return false;
            }
            if (read == 0) {
                continue;
            }
            totalRead += read;
            currentPosition += read;
        }
        return true;
    }

    private byte[] encodeKey(final K key) {
        final int length = keyEncoder.bytesLength(key);
        final byte[] encoded = new byte[length];
        final int written = keyEncoder.toBytes(key, encoded);
        if (written != encoded.length) {
            throw new IllegalStateException(String.format(
                    "Unexpected key encoding length. expected=%s actual=%s",
                    encoded.length, written));
        }
        return encoded;
    }

    private byte[] encodeValue(final V value) {
        final int length = valueEncoder.bytesLength(value);
        final byte[] encoded = new byte[length];
        final int written = valueEncoder.toBytes(value, encoded);
        if (written != encoded.length) {
            throw new IllegalStateException(String.format(
                    "Unexpected value encoding length. expected=%s actual=%s",
                    encoded.length, written));
        }
        return encoded;
    }

    private K decodeKey(final byte[] keyBytes) {
        return keyDecoder.decode(keyBytes);
    }

    private V decodeValue(final byte[] valueBytes) {
        return valueDecoder.decode(valueBytes);
    }

    static int computeCrc32(final byte[] data, final int offset,
            final int length) {
        final CRC32 crc32 = new CRC32();
        crc32.update(data, offset, length);
        return (int) crc32.getValue();
    }

    static void putInt(final byte[] bytes, final int offset, final int value) {
        bytes[offset] = (byte) (value >>> 24);
        bytes[offset + 1] = (byte) (value >>> 16);
        bytes[offset + 2] = (byte) (value >>> 8);
        bytes[offset + 3] = (byte) value;
    }

    static int readInt(final byte[] bytes, final int offset) {
        return ((bytes[offset] & 0xFF) << 24)
                | ((bytes[offset + 1] & 0xFF) << 16)
                | ((bytes[offset + 2] & 0xFF) << 8)
                | (bytes[offset + 3] & 0xFF);
    }

    static void putLong(final byte[] bytes, final int offset, final long value) {
        bytes[offset] = (byte) (value >>> 56);
        bytes[offset + 1] = (byte) (value >>> 48);
        bytes[offset + 2] = (byte) (value >>> 40);
        bytes[offset + 3] = (byte) (value >>> 32);
        bytes[offset + 4] = (byte) (value >>> 24);
        bytes[offset + 5] = (byte) (value >>> 16);
        bytes[offset + 6] = (byte) (value >>> 8);
        bytes[offset + 7] = (byte) value;
    }

    static long readLong(final byte[] bytes, final int offset) {
        return ((long) (bytes[offset] & 0xFF) << 56)
                | ((long) (bytes[offset + 1] & 0xFF) << 48)
                | ((long) (bytes[offset + 2] & 0xFF) << 40)
                | ((long) (bytes[offset + 3] & 0xFF) << 32)
                | ((long) (bytes[offset + 4] & 0xFF) << 24)
                | ((long) (bytes[offset + 5] & 0xFF) << 16)
                | ((long) (bytes[offset + 6] & 0xFF) << 8)
                | ((long) bytes[offset + 7] & 0xFF);
    }

    private record ScanResult(long validBytes, long maxLsn, long lastReplayedLsn,
            boolean invalidTail) {
    }

    private record FormatMeta(int version, int checksum) {
    }

    private static final class SegmentInfo {
        private final String name;
        private final long baseLsn;
        private long sizeBytes;
        private long maxLsn;

        SegmentInfo(final String name, final long baseLsn, final long sizeBytes,
                final long maxLsn) {
            this.name = name;
            this.baseLsn = baseLsn;
            this.sizeBytes = sizeBytes;
            this.maxLsn = maxLsn;
        }

        String name() {
            return name;
        }

        long baseLsn() {
            return baseLsn;
        }

        long sizeBytes() {
            return sizeBytes;
        }

        long maxLsn() {
            return maxLsn;
        }

        void grow(final long bytes, final long lsn) {
            this.sizeBytes += bytes;
            if (lsn > this.maxLsn) {
                this.maxLsn = lsn;
            }
        }
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

    private static final class StreamCloseable implements AutoCloseable {
        private final java.util.stream.Stream<String> stream;

        StreamCloseable(final java.util.stream.Stream<String> stream) {
            this.stream = Vldtn.requireNonNull(stream, "stream");
        }

        java.util.stream.Stream<String> stream() {
            return stream;
        }

        @Override
        public void close() {
            stream.close();
        }
    }
}
