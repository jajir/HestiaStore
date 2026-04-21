package org.hestiastore.index.segmentindex.core.operation;

import java.util.function.LongSupplier;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.observability.Stats;

/**
 * Collects put-specific busy-wait metrics while the generic retry loop is
 * running.
 */
final class PutBusyRetryMonitor {

    private static final String OPERATION_PUT = "put";

    private final boolean enabled;
    private final Stats stats;
    private final LongSupplier nanoTimeSupplier;
    private long busyWaitStartNanos;
    private long busyRetryCount;

    PutBusyRetryMonitor(final String operationName, final Stats stats,
            final LongSupplier nanoTimeSupplier) {
        this.enabled = OPERATION_PUT.equals(operationName);
        this.stats = Vldtn.requireNonNull(stats, "stats");
        this.nanoTimeSupplier = Vldtn.requireNonNull(nanoTimeSupplier,
                "nanoTimeSupplier");
    }

    void observeRetryableStatus(final IndexResultStatus status) {
        if (!enabled || status != IndexResultStatus.BUSY) {
            return;
        }
        if (busyWaitStartNanos == 0L) {
            busyWaitStartNanos = nanoTimeSupplier.getAsLong();
        }
        busyRetryCount++;
    }

    void finish(final IndexException failure) {
        finish(isTimeoutException(failure));
    }

    void finishWithoutFailure() {
        finish(false);
    }

    private void finish(final boolean timedOut) {
        if (!enabled || busyWaitStartNanos == 0L) {
            return;
        }
        stats.addPutBusyRetryCount(busyRetryCount);
        stats.recordPutBusyWaitNanos(Math.max(0L,
                nanoTimeSupplier.getAsLong() - busyWaitStartNanos));
        if (timedOut) {
            stats.recordPutBusyTimeout();
        }
    }

    private boolean isTimeoutException(final IndexException exception) {
        return exception.getMessage() != null
                && exception.getMessage().contains("timed out");
    }
}
