package org.hestiastore.index.segmentindex.core;

import java.util.function.Supplier;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.wal.WalRuntime;

/**
 * Owns public read/write operations, retry loops, and WAL replay semantics.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class IndexOperationCoordinator<K, V> {

    private static final String OPERATION_PUT = "put";
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final Stats stats;
    private final DirectSegmentWriteCoordinator<K, V> directSegmentWriteCoordinator;
    private final DirectSegmentReadCoordinator<K, V> directSegmentReadCoordinator;
    private final IndexWalCoordinator<K, V> walCoordinator;
    private final IndexRetryPolicy retryPolicy;

    IndexOperationCoordinator(final TypeDescriptor<V> valueTypeDescriptor,
            final Stats stats,
            final DirectSegmentWriteCoordinator<K, V> directSegmentWriteCoordinator,
            final DirectSegmentReadCoordinator<K, V> directSegmentReadCoordinator,
            final IndexWalCoordinator<K, V> walCoordinator,
            final IndexRetryPolicy retryPolicy) {
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.stats = Vldtn.requireNonNull(stats, "stats");
        this.directSegmentWriteCoordinator = Vldtn
                .requireNonNull(directSegmentWriteCoordinator,
                        "directSegmentWriteCoordinator");
        this.directSegmentReadCoordinator = Vldtn.requireNonNull(
                directSegmentReadCoordinator, "directSegmentReadCoordinator");
        this.walCoordinator = Vldtn.requireNonNull(walCoordinator,
                "walCoordinator");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
    }

    void put(final K key, final V value) {
        final long startedNanos = System.nanoTime();
        Vldtn.requireNonNull(key, "key");
        Vldtn.requireNonNull(value, "value");
        stats.incPutCx();

        if (valueTypeDescriptor.isTombstone(value)) {
            throw new IllegalArgumentException(String.format(
                    "Can't insert tombstone value '%s' into index", value));
        }
        final long walLsn = walCoordinator.appendPut(key, value);
        finishWriteOperation(OPERATION_PUT,
                retryWhileBusy(
                        () -> directSegmentWriteCoordinator.put(key, value),
                        OPERATION_PUT, false),
                walLsn, startedNanos);
    }

    V get(final K key) {
        final long startedNanos = System.nanoTime();
        Vldtn.requireNonNull(key, "key");
        stats.incGetCx();

        final IndexResult<V> result = retryWhileBusy(
                () -> directSegmentReadCoordinator.get(key), "get", true);
        if (result.getStatus() == IndexResultStatus.OK) {
            stats.recordReadLatencyNanos(System.nanoTime() - startedNanos);
            return result.getValue();
        }
        stats.recordReadLatencyNanos(System.nanoTime() - startedNanos);
        throw newIndexException("get", null, result.getStatus());
    }

    void delete(final K key) {
        final long startedNanos = System.nanoTime();
        Vldtn.requireNonNull(key, "key");
        stats.incDeleteCx();

        final long walLsn = walCoordinator.appendDelete(key);
        finishWriteOperation("delete",
                retryWhileBusy(
                        () -> directSegmentWriteCoordinator.put(key,
                                valueTypeDescriptor.getTombstone()),
                        "delete", false),
                walLsn, startedNanos);
    }

    void replayWalRecord(final WalRuntime.ReplayRecord<K, V> replayRecord) {
        final WalRuntime.ReplayRecord<K, V> nonNullReplayRecord = Vldtn
                .requireNonNull(replayRecord, "replayRecord");
        final V value = nonNullReplayRecord
                .getOperation() == WalRuntime.Operation.PUT
                        ? nonNullReplayRecord.getValue()
                        : valueTypeDescriptor.getTombstone();
        final IndexResult<Void> result = retryWhileBusy(
                () -> directSegmentWriteCoordinator.put(
                        nonNullReplayRecord.getKey(), value),
                "walReplay", false);
        if (result.getStatus() != IndexResultStatus.OK) {
            throw newIndexException("walReplay", null, result.getStatus());
        }
        walCoordinator.recordAppliedLsn(nonNullReplayRecord.getLsn());
    }

    private void finishWriteOperation(final String operation,
            final IndexResult<Void> result, final long walLsn,
            final long startedNanos) {
        if (result.getStatus() == IndexResultStatus.OK) {
            walCoordinator.recordAppliedLsn(walLsn);
            stats.recordWriteLatencyNanos(System.nanoTime() - startedNanos);
            return;
        }
        stats.recordWriteLatencyNanos(System.nanoTime() - startedNanos);
        throw newIndexException(operation, null, result.getStatus());
    }

    private <T> IndexResult<T> retryWhileBusy(
            final Supplier<IndexResult<T>> operation, final String opName,
            final boolean retryClosed) {
        final long startNanos = retryPolicy.startNanos();
        long busyWaitStartNanos = 0L;
        long busyRetryCount = 0L;
        IndexResult<T> result = operation.get();
        while (shouldRetry(result.getStatus(), retryClosed)) {
            if (OPERATION_PUT.equals(opName)
                    && result.getStatus() == IndexResultStatus.BUSY) {
                if (busyWaitStartNanos == 0L) {
                    busyWaitStartNanos = System.nanoTime();
                }
                busyRetryCount++;
            }
            try {
                retryPolicy.backoffOrThrow(startNanos, opName, null);
            } catch (final IndexException e) {
                recordPutBusyWait(opName, busyWaitStartNanos, busyRetryCount,
                        isTimeoutException(e));
                throw e;
            }
            result = operation.get();
        }
        recordPutBusyWait(opName, busyWaitStartNanos, busyRetryCount, false);
        return result;
    }

    private boolean isTimeoutException(final IndexException exception) {
        return exception.getMessage() != null
                && exception.getMessage().contains("timed out");
    }

    private void recordPutBusyWait(final String opName,
            final long busyWaitStartNanos, final long busyRetryCount,
            final boolean timedOut) {
        if (!OPERATION_PUT.equals(opName) || busyWaitStartNanos == 0L) {
            return;
        }
        stats.addPutBusyRetryCx(busyRetryCount);
        stats.recordPutBusyWaitNanos(
                Math.max(0L, System.nanoTime() - busyWaitStartNanos));
        if (timedOut) {
            stats.incPutBusyTimeoutCx();
        }
    }

    private boolean shouldRetry(final IndexResultStatus status,
            final boolean retryClosed) {
        return status == IndexResultStatus.BUSY
                || retryClosed && status == IndexResultStatus.CLOSED;
    }

    private IndexException newIndexException(final String operation,
            final SegmentId segmentId, final IndexResultStatus status) {
        final String target = segmentId == null ? ""
                : String.format(" on segment '%s'", segmentId);
        return new IndexException(
                String.format("Index operation '%s' failed%s: %s", operation,
                        target, status));
    }
}
