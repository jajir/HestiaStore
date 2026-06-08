package org.hestiastore.index.segmentregistry;

import org.hestiastore.index.BusyRetryPolicy;

/**
 * Retry policy for segment-registry maintenance waits.
 *
 * Package-private by design: callers pass configuration to
 * {@link SegmentRegistryBuilder}, and the registry package owns the concrete
 * retry semantics behind its builder entry point.
 */
final class RegistryMaintenanceRetryPolicy extends BusyRetryPolicy {

    RegistryMaintenanceRetryPolicy(final int backoffMillis,
            final int timeoutMillis) {
        super(backoffMillis, timeoutMillis);
    }

    @Override
    protected String formatOperationLabel() {
        return "Maintenance operation";
    }
}
