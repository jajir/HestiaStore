package org.hestiastore.index.segmentindex;

import java.util.concurrent.TimeUnit;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;

/**
 * Backoff/timeout policy for retrying BUSY index operations.
 */
final class IndexRetryPolicy {

    private final int backoffMillis;
    private final int timeoutMillis;
    private final long timeoutNanos;

    IndexRetryPolicy(final int backoffMillis, final int timeoutMillis) {
        this.backoffMillis = Vldtn.requireGreaterThanZero(backoffMillis,
                "indexBusyBackoffMillis");
        this.timeoutMillis = Vldtn.requireGreaterThanZero(timeoutMillis,
                "indexBusyTimeoutMillis");
        this.timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    }

    long startNanos() {
        return System.nanoTime();
    }

    void backoffOrThrow(final long startNanos, final String operation,
            final SegmentId segmentId) {
        if (hasTimedOut(startNanos)) {
            throw new IndexException(formatTimeoutMessage(operation,
                    segmentId));
        }
        try {
            Thread.sleep(backoffMillis);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IndexException(formatInterruptedMessage(operation,
                    segmentId), e);
        }
    }

    private boolean hasTimedOut(final long startNanos) {
        return System.nanoTime() - startNanos >= timeoutNanos;
    }

    private String formatTimeoutMessage(final String operation,
            final SegmentId segmentId) {
        final String target = segmentId == null ? "" : String
                .format(" on segment '%s'", segmentId);
        return String.format(
                "Index operation '%s' timed out after %d ms%s", operation,
                timeoutMillis, target);
    }

    private String formatInterruptedMessage(final String operation,
            final SegmentId segmentId) {
        final String target = segmentId == null ? "" : String
                .format(" on segment '%s'", segmentId);
        return String.format("Index operation '%s' was interrupted%s",
                operation, target);
    }
}
