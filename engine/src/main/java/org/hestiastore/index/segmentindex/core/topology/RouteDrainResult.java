package org.hestiastore.index.segmentindex.core.topology;

import org.hestiastore.index.Vldtn;

/**
 * Route drain acquisition result.
 */
public final class RouteDrainResult {

    private final RouteDrain drain;

    private RouteDrainResult(final RouteDrain drain) {
        this.drain = drain;
    }

    static RouteDrainResult acquired(final RouteDrain drain) {
        return new RouteDrainResult(Vldtn.requireNonNull(drain, "drain"));
    }

    static RouteDrainResult routeUnavailable() {
        return new RouteDrainResult(null);
    }

    public boolean isAcquired() {
        return drain != null;
    }

    public RouteDrain drain() {
        if (!isAcquired()) {
            throw new IllegalStateException("Route drain is not available.");
        }
        return drain;
    }
}
