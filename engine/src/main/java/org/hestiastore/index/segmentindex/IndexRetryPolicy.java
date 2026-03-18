package org.hestiastore.index.segmentindex;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;

/**
 * Backoff/timeout policy for retrying BUSY index operations.
 */
public final class IndexRetryPolicy {

    private final int backoffMillis;
    private final int timeoutMillis;
    private final long backoffNanos;
    private final long maxJitterNanos;
    private final long timeoutNanos;

    /**
     * Creates a retry policy for BUSY operations.
     *
     * @param backoffMillis sleep duration between retries
     * @param timeoutMillis overall timeout budget for a retry loop
     */
    public IndexRetryPolicy(final int backoffMillis,
            final int timeoutMillis) {
        this.backoffMillis = Vldtn.requireGreaterThanZero(backoffMillis,
                "indexBusyBackoffMillis");
        this.timeoutMillis = Vldtn.requireGreaterThanZero(timeoutMillis,
                "indexBusyTimeoutMillis");
        this.backoffNanos = TimeUnit.MILLISECONDS.toNanos(backoffMillis);
        this.maxJitterNanos = Math.max(1L, backoffNanos / 4L);
        this.timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    }

    /**
     * Captures the start timestamp for a retry loop.
     *
     * @return start timestamp in nanoseconds
     */
    public long startNanos() {
        return System.nanoTime();
    }

    /**
     * Sleeps for one backoff interval or throws when timeout/interruption is
     * reached.
     *
     * @param startNanos retry loop start timestamp
     * @param operation operation label used in error messages
     * @param segmentId optional segment id for error context
     */
    public void backoffOrThrow(final long startNanos, final String operation,
            final SegmentId segmentId) {
        if (hasTimedOut(startNanos)) {
            throw new IndexException(formatTimeoutMessage(operation,
                    segmentId));
        }
        LockSupport.parkNanos(nextBackoffNanos());
        if (Thread.interrupted()) {
            Thread.currentThread().interrupt();
            throw new IndexException(formatInterruptedMessage(operation,
                    segmentId));
        }
        if (hasTimedOut(startNanos)) {
            throw new IndexException(formatTimeoutMessage(operation,
                    segmentId));
        }
    }

    private long nextBackoffNanos() {
        return backoffNanos + ThreadLocalRandom.current()
                .nextLong(maxJitterNanos);
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
