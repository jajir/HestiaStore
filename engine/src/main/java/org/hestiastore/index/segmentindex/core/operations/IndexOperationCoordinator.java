package org.hestiastore.index.segmentindex.core.operations;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segmentindex.core.segmentlease.SegmentLease;
import org.hestiastore.index.segmentindex.core.segmentlease.SegmentLeaseService;
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

    private final IndexOperationStatsRecorder statsRecorder;
    private final SegmentLeaseService<K, V> segmentLeaseService;
    private final IndexWalCoordinator<K, V> walCoordinator;
    private final TypeDescriptor<V> valueTypeDescriptor;

    IndexOperationCoordinator(final TypeDescriptor<V> valueTypeDescriptor,
            final IndexOperationStatsRecorder statsRecorder,
            final SegmentLeaseService<K, V> segmentLeaseService,
            final IndexWalCoordinator<K, V> walCoordinator) {
        this.statsRecorder = Vldtn.requireNonNull(statsRecorder,
                "statsRecorder");
        this.segmentLeaseService = Vldtn.requireNonNull(segmentLeaseService,
                "segmentLeaseService");
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
        statsRecorder.recordPutRequest();
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
        statsRecorder.recordGetRequest();
        final V result = readFromSegment(nonNullKey);
        recordReadLatency(startedNanos);
        return result;
    }

    @Override
    public void delete(final K key) {
        final long startedNanos = startWriteOperation();
        final K nonNullKey = requireKey(key);
        statsRecorder.recordDeleteRequest();
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
        final SegmentLease<K, V> lease = segmentLeaseService.acquireForRead(
                key);
        if (lease == null) {
            return null;
        }
        try (SegmentLease<K, V> activeLease = lease) {
            return activeLease.segment().get(key);
        }
    }

    private void writeToSegment(final K key, final V value) {
        try (SegmentLease<K, V> lease = segmentLeaseService
                .acquireForWrite(key)) {
            lease.segment().put(key, value);
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
        statsRecorder.recordReadLatencyNanos(System.nanoTime() - startedNanos);
    }

    private void recordWriteLatency(final long startedNanos) {
        statsRecorder.recordWriteLatencyNanos(
                System.nanoTime() - startedNanos);
    }
}
