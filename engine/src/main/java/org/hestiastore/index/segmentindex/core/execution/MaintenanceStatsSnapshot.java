package org.hestiastore.index.segmentindex.core.execution;

import org.hestiastore.index.Vldtn;

/**
 * Immutable maintenance runtime statistics.
 */
public final class MaintenanceStatsSnapshot {

    private final long flushRequestCount;
    private final long compactRequestCount;
    private final long flushBusyRetryCount;
    private final long compactBusyRetryCount;
    private final long flushAcceptedToReadyP95Micros;
    private final long compactAcceptedToReadyP95Micros;

    public MaintenanceStatsSnapshot(final long flushRequestCount,
            final long compactRequestCount, final long flushBusyRetryCount,
            final long compactBusyRetryCount,
            final long flushAcceptedToReadyP95Micros,
            final long compactAcceptedToReadyP95Micros) {
        this.flushRequestCount = Vldtn.requireGreaterThanOrEqualToZero(
                flushRequestCount, "flushRequestCount");
        this.compactRequestCount = Vldtn.requireGreaterThanOrEqualToZero(
                compactRequestCount, "compactRequestCount");
        this.flushBusyRetryCount = Vldtn.requireGreaterThanOrEqualToZero(
                flushBusyRetryCount, "flushBusyRetryCount");
        this.compactBusyRetryCount = Vldtn.requireGreaterThanOrEqualToZero(
                compactBusyRetryCount, "compactBusyRetryCount");
        this.flushAcceptedToReadyP95Micros =
                Vldtn.requireGreaterThanOrEqualToZero(
                        flushAcceptedToReadyP95Micros,
                        "flushAcceptedToReadyP95Micros");
        this.compactAcceptedToReadyP95Micros =
                Vldtn.requireGreaterThanOrEqualToZero(
                        compactAcceptedToReadyP95Micros,
                        "compactAcceptedToReadyP95Micros");
    }

    public long getFlushRequestCount() {
        return flushRequestCount;
    }

    public long getCompactRequestCount() {
        return compactRequestCount;
    }

    public long getFlushBusyRetryCount() {
        return flushBusyRetryCount;
    }

    public long getCompactBusyRetryCount() {
        return compactBusyRetryCount;
    }

    public long getFlushAcceptedToReadyP95Micros() {
        return flushAcceptedToReadyP95Micros;
    }

    public long getCompactAcceptedToReadyP95Micros() {
        return compactAcceptedToReadyP95Micros;
    }
}
