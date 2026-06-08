package org.hestiastore.index.segmentindex.core.split;

import org.hestiastore.index.BusyRetryPolicy;

/**
 * Retry policy for split preparation operations.
 *
 * Package-private by design: split callers configure retry timing through
 * {@link SplitServiceBuilder}, and the split package keeps the concrete retry
 * semantics local to the code that uses them.
 */
final class SplitRetryPolicy extends BusyRetryPolicy {

    SplitRetryPolicy(final int backoffMillis, final int timeoutMillis) {
        super(backoffMillis, timeoutMillis);
    }

    @Override
    protected String formatOperationLabel() {
        return "Split operation";
    }
}
