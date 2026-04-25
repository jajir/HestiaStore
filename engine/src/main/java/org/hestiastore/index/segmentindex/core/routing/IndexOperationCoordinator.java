package org.hestiastore.index.segmentindex.core.routing;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segmentindex.core.metrics.Stats;
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
    private final DirectSegmentAccess<K, V> directSegmentCoordinator;
    private final IndexWalCoordinator<K, V> walCoordinator;
    private final TypeDescriptor<V> valueTypeDescriptor;

    IndexOperationCoordinator(final TypeDescriptor<V> valueTypeDescriptor,
            final Stats stats,
            final DirectSegmentAccess<K, V> directSegmentCoordinator,
            final IndexWalCoordinator<K, V> walCoordinator) {
        this.stats = Vldtn.requireNonNull(stats, "stats");
        this.directSegmentCoordinator = Vldtn.requireNonNull(
                directSegmentCoordinator, "directSegmentCoordinator");
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
        directSegmentCoordinator.put(nonNullKey, nonNullValue);
        walCoordinator.recordAppliedLsn(walLsn);
        recordWriteLatency(startedNanos);
    }

    @Override
    public V get(final K key) {
        final long startedNanos = startReadOperation();
        final K nonNullKey = requireKey(key);
        stats.recordGetRequest();
        final V result = directSegmentCoordinator.get(nonNullKey);
        recordReadLatency(startedNanos);
        return result;
    }

    @Override
    public void delete(final K key) {
        final long startedNanos = startWriteOperation();
        final K nonNullKey = requireKey(key);
        stats.recordDeleteRequest();
        final long walLsn = walCoordinator.appendDelete(nonNullKey);
        directSegmentCoordinator.put(nonNullKey, tombstoneValue());
        walCoordinator.recordAppliedLsn(walLsn);
        recordWriteLatency(startedNanos);
    }

    @Override
    public void replayWalRecord(
            final WalRuntime.ReplayRecord<K, V> replayRecord) {
        final WalRuntime.ReplayRecord<K, V> nonNullReplayRecord = Vldtn
                .requireNonNull(replayRecord, "replayRecord");
        directSegmentCoordinator.put(nonNullReplayRecord.getKey(),
                replayValue(nonNullReplayRecord));
        walCoordinator.recordAppliedLsn(nonNullReplayRecord.getLsn());
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
