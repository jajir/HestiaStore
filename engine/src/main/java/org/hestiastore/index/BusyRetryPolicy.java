package org.hestiastore.index;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Backoff/timeout policy for retrying BUSY operations.
 */
public class BusyRetryPolicy {

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
    public BusyRetryPolicy(final int backoffMillis, final int timeoutMillis) {
        Vldtn.requireGreaterThanZero(backoffMillis, "busyBackoffMillis");
        this.timeoutMillis = Vldtn.requireGreaterThanZero(timeoutMillis,
                "busyTimeoutMillis");
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
     * @param target optional retry target for error context
     */
    public void backoffOrThrow(final long startNanos, final String operation,
            final Object target) {
        if (hasTimedOut(startNanos)) {
            throw new IndexException(formatTimeoutMessage(operation, target));
        }
        LockSupport.parkNanos(nextBackoffNanos());
        if (Thread.interrupted()) {
            Thread.currentThread().interrupt();
            throw new IndexException(
                    formatInterruptedMessage(operation, target));
        }
        if (hasTimedOut(startNanos)) {
            throw new IndexException(formatTimeoutMessage(operation, target));
        }
    }

    private long nextBackoffNanos() {
        if (maxJitterNanos <= 1L) {
            return backoffNanos;
        }
        return backoffNanos + Long.remainderUnsigned(
                mix64(System.nanoTime() ^ Thread.currentThread().getId()),
                maxJitterNanos);
    }

    private long mix64(final long value) {
        long mixed = value;
        mixed ^= mixed >>> 33;
        mixed *= 0xff51afd7ed558ccdL;
        mixed ^= mixed >>> 33;
        mixed *= 0xc4ceb9fe1a85ec53L;
        mixed ^= mixed >>> 33;
        return mixed;
    }

    private boolean hasTimedOut(final long startNanos) {
        return System.nanoTime() - startNanos >= timeoutNanos;
    }

    protected String formatTimeoutMessage(final String operation,
            final Object target) {
        return String.format("%s '%s' timed out after %d ms%s",
                formatOperationLabel(), operation, timeoutMillis,
                formatTarget(target));
    }

    protected String formatInterruptedMessage(final String operation,
            final Object target) {
        return String.format("%s '%s' was interrupted%s",
                formatOperationLabel(), operation, formatTarget(target));
    }

    /**
     * Formats optional target context used in error messages.
     *
     * @param target optional retry target
     * @return formatted suffix or empty string
     */
    protected String formatTarget(final Object target) {
        return target == null ? ""
                : String.format(" on target '%s'", target);
    }

    /**
     * Formats the operation label prefix used in error messages.
     *
     * @return operation label prefix
     */
    protected String formatOperationLabel() {
        return "Operation";
    }
}
