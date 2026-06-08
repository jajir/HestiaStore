package org.hestiastore.index.segmentindex.core.storage;

import org.hestiastore.index.BusyRetryPolicy;

/**
 * Retry policy for cleanup of physical storage that is no longer routed.
 *
 * Package-private by design: external modules call {@link StorageService}, and
 * the storage package owns the concrete cleanup retry semantics behind that
 * access point.
 */
final class StorageCleanupRetryPolicy extends BusyRetryPolicy {

    StorageCleanupRetryPolicy(final int backoffMillis,
            final int timeoutMillis) {
        super(backoffMillis, timeoutMillis);
    }

    @Override
    protected String formatOperationLabel() {
        return "Storage cleanup operation";
    }
}
