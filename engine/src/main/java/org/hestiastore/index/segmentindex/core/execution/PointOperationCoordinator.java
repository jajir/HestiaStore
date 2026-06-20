package org.hestiastore.index.segmentindex.core.execution;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segmentindex.core.routing.MappedSegmentLease;
import org.hestiastore.index.segmentindex.core.routing.MappedSegmentLeaseService;
import org.hestiastore.index.segmentindex.core.storage.StorageCoordinator;
import org.hestiastore.index.segmentindex.wal.WalRuntime;

/**
 * Owns public read/write operations and WAL replay semantics.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class PointOperationCoordinator<K, V> {

    private final IndexOperationStatsRecorder statsRecorder;
    private final MappedSegmentLeaseService<K, V> segmentLeaseService;
    private final StorageCoordinator<K, V> storageService;
    private final TypeDescriptor<V> valueTypeDescriptor;

    /**
     * Creates an operation coordinator from initialized runtime services.
     *
     * @param valueTypeDescriptor value descriptor used for tombstones
     * @param statsRecorder operation metrics recorder
     * @param segmentLeaseService segment lease service
     * @param storageService storage and WAL service
     */
    public PointOperationCoordinator(
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexOperationStatsRecorder statsRecorder,
            final MappedSegmentLeaseService<K, V> segmentLeaseService,
            final StorageCoordinator<K, V> storageService) {
        this.statsRecorder = Vldtn.requireNonNull(statsRecorder,
                "statsRecorder");
        this.segmentLeaseService = Vldtn.requireNonNull(segmentLeaseService,
                "segmentLeaseService");
        this.storageService = Vldtn.requireNonNull(storageService,
                "storageService");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
    }

    /**
     * Stores a value for a key and records the WAL entry as applied.
     *
     * @param key key to store
     * @param value value to store
     */
    public void put(final K key, final V value) {
        final long startedNanos = startWriteOperation();
        final K nonNullKey = requireKey(key);
        final V nonNullValue = requireValue(value);
        statsRecorder.recordPutRequest();
        rejectTombstoneValue(nonNullValue);
        final long walLsn = storageService.appendWalPut(nonNullKey,
                nonNullValue);
        writeToSegment(nonNullKey, nonNullValue);
        storageService.recordAppliedWalLsn(walLsn);
        recordWriteLatency(startedNanos);
    }

    /**
     * Reads the value currently stored for a key.
     *
     * @param key key to read
     * @return stored value or {@code null} when the key is absent
     */
    public V get(final K key) {
        final long startedNanos = startReadOperation();
        final K nonNullKey = requireKey(key);
        statsRecorder.recordGetRequest();
        final V result = readFromSegment(nonNullKey);
        recordReadLatency(startedNanos);
        return result;
    }

    /**
     * Deletes a key by writing its tombstone value.
     *
     * @param key key to delete
     */
    public void delete(final K key) {
        final long startedNanos = startWriteOperation();
        final K nonNullKey = requireKey(key);
        statsRecorder.recordDeleteRequest();
        final long walLsn = storageService.appendWalDelete(nonNullKey);
        writeToSegment(nonNullKey, tombstoneValue());
        storageService.recordAppliedWalLsn(walLsn);
        recordWriteLatency(startedNanos);
    }

    /**
     * Replays one WAL record into the segment layer.
     *
     * @param replayRecord WAL record to replay
     */
    public void replayWalRecord(
            final WalRuntime.ReplayRecord<K, V> replayRecord) {
        final WalRuntime.ReplayRecord<K, V> nonNullReplayRecord = Vldtn
                .requireNonNull(replayRecord, "replayRecord");
        writeToSegment(nonNullReplayRecord.getKey(),
                replayValue(nonNullReplayRecord));
        storageService.recordAppliedWalLsn(nonNullReplayRecord.getLsn());
    }

    private V readFromSegment(final K key) {
        final MappedSegmentLease<K, V> lease = segmentLeaseService.acquireForRead(
                key);
        if (lease == null) {
            return null;
        }
        try (MappedSegmentLease<K, V> activeLease = lease) {
            return activeLease.segment().get(key);
        }
    }

    private void writeToSegment(final K key, final V value) {
        try (MappedSegmentLease<K, V> lease = segmentLeaseService
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
