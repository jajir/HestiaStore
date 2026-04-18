package org.hestiastore.index.segmentindex;

import org.hestiastore.index.BusyRetryPolicy;

/**
 * Backward-compatible index-specific alias for {@link BusyRetryPolicy}.
 */
public class IndexRetryPolicy extends BusyRetryPolicy {

    /**
     * Creates a retry policy for BUSY operations.
     *
     * @param backoffMillis sleep duration between retries
     * @param timeoutMillis overall timeout budget for a retry loop
     */
    public IndexRetryPolicy(final int backoffMillis,
            final int timeoutMillis) {
        super(backoffMillis, timeoutMillis);
    }

    /** {@inheritDoc} */
    @Override
    protected String formatTarget(final Object target) {
        return target == null ? ""
                : String.format(" on segment '%s'", target);
    }

    /** {@inheritDoc} */
    @Override
    protected String formatOperationLabel() {
        return "Index operation";
    }
}
