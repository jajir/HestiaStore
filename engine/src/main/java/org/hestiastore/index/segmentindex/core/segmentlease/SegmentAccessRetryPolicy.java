package org.hestiastore.index.segmentindex.core.segmentlease;

import org.hestiastore.index.BusyRetryPolicy;

/**
 * Retry policy for segment access and route lease acquisition.
 *
 * Package-private by design: callers pass retry values to
 * {@link SegmentLeaseServiceBuilder}, and the segment-lease package owns the
 * concrete retry semantics behind its service entry point.
 */
final class SegmentAccessRetryPolicy extends BusyRetryPolicy {

    SegmentAccessRetryPolicy(final int backoffMillis,
            final int timeoutMillis) {
        super(backoffMillis, timeoutMillis);
    }

    @Override
    protected String formatOperationLabel() {
        return "Segment access operation";
    }
}
