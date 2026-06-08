package org.hestiastore.index.segmentindex.core.maintenance;

import org.hestiastore.index.BusyRetryPolicy;

/**
 * Retry policy for index-level maintenance operations.
 *
 * Package-private by design: callers pass retry values to
 * {@link MaintenanceServiceBuilder}, and the maintenance package owns the
 * concrete retry semantics behind its service entry point.
 */
final class MaintenanceRetryPolicy extends BusyRetryPolicy {

    MaintenanceRetryPolicy(final int backoffMillis,
            final int timeoutMillis) {
        super(backoffMillis, timeoutMillis);
    }

    @Override
    protected String formatOperationLabel() {
        return "Maintenance operation";
    }
}
