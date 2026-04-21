package org.hestiastore.index.segmentindex.core.operation;

import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.core.durability.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.core.observability.Stats;
import org.hestiastore.index.segmentindex.wal.WalRuntime;

/**
 * Owns public read/write operations, retry loops, and WAL replay semantics.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class IndexOperationCoordinator<K, V>
        implements SegmentIndexOperationAccess<K, V> {

    private final Stats stats;
    private final DirectSegmentAccess<K, V> directSegmentCoordinator;
    private final IndexWalCoordinator<K, V> walCoordinator;
    private final IndexOperationOutcomeHandler<K, V> outcomeHandler;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final IndexRetryPolicy retryPolicy;

    IndexOperationCoordinator(final TypeDescriptor<V> valueTypeDescriptor,
            final Stats stats,
            final DirectSegmentAccess<K, V> directSegmentCoordinator,
            final IndexWalCoordinator<K, V> walCoordinator,
            final IndexRetryPolicy retryPolicy) {
        this.stats = Vldtn.requireNonNull(stats, "stats");
        this.directSegmentCoordinator = Vldtn.requireNonNull(
                directSegmentCoordinator, "directSegmentCoordinator");
        this.walCoordinator = Vldtn.requireNonNull(walCoordinator,
                "walCoordinator");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
        this.outcomeHandler = new IndexOperationOutcomeHandler<>(this.stats,
                this.walCoordinator);
    }

    @Override
    public void put(final K key, final V value) {
        final long startedNanos = startWriteOperation();
        final K nonNullKey = requireKey(key);
        final V nonNullValue = requireValue(value);
        stats.recordPutRequest();
        rejectTombstoneValue(nonNullValue);
        final long walLsn = walCoordinator.appendPut(nonNullKey, nonNullValue);
        final IndexResult<Void> result = retryWrite(
                () -> directSegmentCoordinator.put(nonNullKey,
                        nonNullValue),
                "put");
        outcomeHandler.finishWrite("put", result, walLsn, startedNanos);
    }

    @Override
    public V get(final K key) {
        final long startedNanos = startReadOperation();
        final K nonNullKey = requireKey(key);
        stats.recordGetRequest();
        final IndexResult<V> result = retryRead(
                () -> directSegmentCoordinator.get(nonNullKey));
        return outcomeHandler.finishRead("get", result, startedNanos);
    }

    @Override
    public void delete(final K key) {
        final long startedNanos = startWriteOperation();
        final K nonNullKey = requireKey(key);
        stats.recordDeleteRequest();
        final long walLsn = walCoordinator.appendDelete(nonNullKey);
        final IndexResult<Void> result = retryWrite(
                () -> directSegmentCoordinator.put(nonNullKey,
                        tombstoneValue()),
                "delete");
        outcomeHandler.finishWrite("delete", result, walLsn, startedNanos);
    }

    @Override
    public void replayWalRecord(
            final WalRuntime.ReplayRecord<K, V> replayRecord) {
        final WalRuntime.ReplayRecord<K, V> nonNullReplayRecord = Vldtn
                .requireNonNull(replayRecord, "replayRecord");
        final IndexResult<Void> result = retryWrite(
                () -> directSegmentCoordinator.put(
                        nonNullReplayRecord.getKey(),
                        replayValue(nonNullReplayRecord)),
                "walReplay");
        if (result.getStatus() != IndexResultStatus.OK) {
            throw outcomeHandler.newIndexException("walReplay", null,
                    result.getStatus());
        }
        walCoordinator.recordAppliedLsn(nonNullReplayRecord.getLsn());
    }

    private long startWriteOperation() {
        return System.nanoTime();
    }

    private long startReadOperation() {
        return System.nanoTime();
    }

    private IndexResult<Void> retryWrite(
            final Supplier<IndexResult<Void>> operation,
            final String operationName) {
        return retryWhileBusy(operation, operationName, false);
    }

    private <T> IndexResult<T> retryRead(
            final Supplier<IndexResult<T>> operation) {
        return retryWhileBusy(operation, "get", true);
    }

    private <T> IndexResult<T> retryWhileBusy(
            final Supplier<IndexResult<T>> operation, final String opName,
            final boolean retryClosed) {
        final PutBusyRetryMonitor putBusyRetryMonitor =
                new PutBusyRetryMonitor(opName, stats, System::nanoTime);
        final long startNanos = retryPolicy.startNanos();
        IndexResult<T> result = operation.get();
        while (shouldRetry(result.getStatus(), retryClosed)) {
            putBusyRetryMonitor.observeRetryableStatus(result.getStatus());
            try {
                retryPolicy.backoffOrThrow(startNanos, opName, null);
            } catch (final org.hestiastore.index.IndexException e) {
                putBusyRetryMonitor.finish(e);
                throw e;
            }
            result = operation.get();
        }
        putBusyRetryMonitor.finishWithoutFailure();
        return result;
    }

    private boolean shouldRetry(final IndexResultStatus status,
            final boolean retryClosed) {
        return status == IndexResultStatus.BUSY
                || retryClosed && status == IndexResultStatus.CLOSED;
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
}
