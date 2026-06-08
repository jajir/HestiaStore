package org.hestiastore.index.segmentindex.core.storage;

import org.hestiastore.index.BusyRetryPolicy;

/**
 * Retry policy for WAL retention backpressure.
 *
 * Package-private by design: external modules call {@link StorageService}, and
 * the storage package owns the concrete WAL retry semantics behind that access
 * point.
 */
final class WalBackpressureRetryPolicy extends BusyRetryPolicy {

    WalBackpressureRetryPolicy(final int backoffMillis,
            final int timeoutMillis) {
        super(backoffMillis, timeoutMillis);
    }

    @Override
    protected String formatOperationLabel() {
        return "WAL backpressure operation";
    }
}
