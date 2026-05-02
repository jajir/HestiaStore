package org.hestiastore.index.segmentindex.core.operations;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segmentindex.metrics.Stats;
import org.hestiastore.index.segmentindex.core.segmentaccess.SegmentAccess;
import org.hestiastore.index.segmentindex.core.segmentaccess.SegmentAccessService;
import org.hestiastore.index.segmentindex.core.storage.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.wal.WalRuntime;

/**
 * Owns public read/write operations and WAL replay semantics.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class IndexOperationCoordinator<K, V>
        implements SegmentIndexOperationAccess<K, V> {

    private final Stats stats;
    private final SegmentAccessService<K, V> segmentAccessService;
    private final IndexWalCoordinator<K, V> walCoordinator;
    private final TypeDescriptor<V> valueTypeDescriptor;

    IndexOperationCoordinator(final TypeDescriptor<V> valueTypeDescriptor,
            final Stats stats,
            final SegmentAccessService<K, V> segmentAccessService,
            final IndexWalCoordinator<K, V> walCoordinator) {
        this.stats = Vldtn.requireNonNull(stats, "stats");
        this.segmentAccessService = Vldtn.requireNonNull(segmentAccessService,
                "segmentAccessService");
        this.walCoordinator = Vldtn.requireNonNull(walCoordinator,
                "walCoordinator");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
    }

    @Override
    public void put(final K key, final V value) {
        final long startedNanos = startWriteOperation();
        final K nonNullKey = requireKey(key);
        final V nonNullValue = requireValue(value);
        stats.recordPutRequest();
        rejectTombstoneValue(nonNullValue);
        final long walLsn = walCoordinator.appendPut(nonNullKey, nonNullValue);
        writeToSegment(nonNullKey, nonNullValue);
        walCoordinator.recordAppliedLsn(walLsn);
        recordWriteLatency(startedNanos);
    }

    @Override
    public V get(final K key) {
        final long startedNanos = startReadOperation();
        final K nonNullKey = requireKey(key);
        stats.recordGetRequest();
        final V result = readFromSegment(nonNullKey);
        recordReadLatency(startedNanos);
        return result;
    }

    @Override
    public void delete(final K key) {
        final long startedNanos = startWriteOperation();
        final K nonNullKey = requireKey(key);
        stats.recordDeleteRequest();
        final long walLsn = walCoordinator.appendDelete(nonNullKey);
        writeToSegment(nonNullKey, tombstoneValue());
        walCoordinator.recordAppliedLsn(walLsn);
        recordWriteLatency(startedNanos);
    }

    @Override
    public void replayWalRecord(
            final WalRuntime.ReplayRecord<K, V> replayRecord) {
        final WalRuntime.ReplayRecord<K, V> nonNullReplayRecord = Vldtn
                .requireNonNull(replayRecord, "replayRecord");
        writeToSegment(nonNullReplayRecord.getKey(),
                replayValue(nonNullReplayRecord));
        walCoordinator.recordAppliedLsn(nonNullReplayRecord.getLsn());
    }

    private V readFromSegment(final K key) {
        final SegmentAccess<K, V> access = segmentAccessService.acquireForRead(
                key);
        if (access == null) {
            return null;
        }
        try (SegmentAccess<K, V> activeAccess = access) {
            return activeAccess.segment().get(key);
        }
    }

    private void writeToSegment(final K key, final V value) {
        try (SegmentAccess<K, V> access = segmentAccessService
                .acquireForWrite(key)) {
            access.segment().put(key, value);
        }
    }

    private long startWriteOperation() {
        return System.nanoTime();
    }

    private long startReadOperation() {
        return System.nanoTime();
    }

    private K requireKey(final K key) {
        return Vldtn.requireNonNull(key, "key");
    }

    private V requireValue(final V value) {
        return Vldtn.requireNonNull(value, "value");
    }

    private void rejectTombstoneValue(final V value) {
        if (!valueTypeDescriptor.isTombstone(value)) {
            return;
        }
        throw new IllegalArgumentException(String.format(
                "Can't insert tombstone value '%s' into index", value));
    }

    private V tombstoneValue() {
        return valueTypeDescriptor.getTombstone();
    }

    private V replayValue(final WalRuntime.ReplayRecord<K, V> replayRecord) {
        return replayRecord.getOperation() == WalRuntime.Operation.PUT
                ? replayRecord.getValue()
                : tombstoneValue();
    }

    private void recordReadLatency(final long startedNanos) {
        stats.recordReadLatencyNanos(System.nanoTime() - startedNanos);
    }

    private void recordWriteLatency(final long startedNanos) {
        stats.recordWriteLatencyNanos(System.nanoTime() - startedNanos);
    }
}
