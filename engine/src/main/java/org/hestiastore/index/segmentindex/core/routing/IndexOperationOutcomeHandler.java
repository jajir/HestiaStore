package org.hestiastore.index.segmentindex.core.routing;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.core.storage.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.core.metrics.Stats;

/**
 * Applies common success/failure handling for read and write operations.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class IndexOperationOutcomeHandler<K, V> {

    private final Stats stats;
    private final IndexWalCoordinator<K, V> walCoordinator;

    IndexOperationOutcomeHandler(final Stats stats,
            final IndexWalCoordinator<K, V> walCoordinator) {
        this.stats = Vldtn.requireNonNull(stats, "stats");
        this.walCoordinator = Vldtn.requireNonNull(walCoordinator,
                "walCoordinator");
    }

    <T> T finishRead(final String operation, final IndexResult<T> result,
            final long startedNanos) {
        recordReadLatency(startedNanos);
        if (result.getStatus() == IndexResultStatus.OK) {
            return result.getValue();
        }
        throw newIndexException(operation, null, result.getStatus());
    }

    void finishWrite(final String operation, final IndexResult<Void> result,
            final long walLsn, final long startedNanos) {
        if (result.getStatus() == IndexResultStatus.OK) {
            walCoordinator.recordAppliedLsn(walLsn);
            recordWriteLatency(startedNanos);
            return;
        }
        recordWriteLatency(startedNanos);
        throw newIndexException(operation, null, result.getStatus());
    }

    IndexException newIndexException(final String operation,
            final SegmentId segmentId, final IndexResultStatus status) {
        final String target = segmentId == null ? ""
                : String.format(" on segment '%s'", segmentId);
        return new IndexException(
                String.format("Index operation '%s' failed%s: %s", operation,
                        target, status));
    }

    private void recordReadLatency(final long startedNanos) {
        stats.recordReadLatencyNanos(System.nanoTime() - startedNanos);
    }

    private void recordWriteLatency(final long startedNanos) {
        stats.recordWriteLatencyNanos(System.nanoTime() - startedNanos);
    }
}
