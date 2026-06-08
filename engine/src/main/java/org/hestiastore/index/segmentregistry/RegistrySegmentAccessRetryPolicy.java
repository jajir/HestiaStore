package org.hestiastore.index.segmentregistry;

import org.hestiastore.index.BusyRetryPolicy;

/**
 * Retry policy for blocking segment access through the registry.
 *
 * Package-private by design: callers pass configuration to
 * {@link SegmentRegistryBuilder}, and the registry package owns the concrete
 * retry semantics behind its builder entry point.
 */
final class RegistrySegmentAccessRetryPolicy extends BusyRetryPolicy {

    RegistrySegmentAccessRetryPolicy(final int backoffMillis,
            final int timeoutMillis) {
        super(backoffMillis, timeoutMillis);
    }

    @Override
    protected String formatOperationLabel() {
        return "Segment access operation";
    }
}
