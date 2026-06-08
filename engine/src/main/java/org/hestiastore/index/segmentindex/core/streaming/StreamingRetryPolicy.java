package org.hestiastore.index.segmentindex.core.streaming;

import org.hestiastore.index.BusyRetryPolicy;

/**
 * Retry policy for routed streaming operations.
 *
 * Package-private by design: callers pass retry values to streaming builders or
 * factories, and the streaming package owns the concrete retry semantics behind
 * its access points.
 */
final class StreamingRetryPolicy extends BusyRetryPolicy {

    StreamingRetryPolicy(final int backoffMillis, final int timeoutMillis) {
        super(backoffMillis, timeoutMillis);
    }

    @Override
    protected String formatOperationLabel() {
        return "Streaming operation";
    }
}
