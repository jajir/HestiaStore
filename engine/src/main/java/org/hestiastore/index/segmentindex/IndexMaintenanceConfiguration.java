package org.hestiastore.index.segmentindex;

/**
 * Immutable maintenance, lifecycle, and retry settings view.
 */
public final class IndexMaintenanceConfiguration {

    private final Integer segmentThreads;
    private final Integer indexThreads;
    private final Integer registryLifecycleThreads;
    private final Integer busyBackoffMillis;
    private final Integer busyTimeoutMillis;
    private final Boolean backgroundAutoEnabled;

    public IndexMaintenanceConfiguration(final Integer segmentThreads,
            final Integer indexThreads,
            final Integer registryLifecycleThreads,
            final Integer busyBackoffMillis,
            final Integer busyTimeoutMillis,
            final Boolean backgroundAutoEnabled) {
        this.segmentThreads = segmentThreads;
        this.indexThreads = indexThreads;
        this.registryLifecycleThreads = registryLifecycleThreads;
        this.busyBackoffMillis = busyBackoffMillis;
        this.busyTimeoutMillis = busyTimeoutMillis;
        this.backgroundAutoEnabled = backgroundAutoEnabled;
    }

    public Integer segmentThreads() {
        return segmentThreads;
    }

    public Integer indexThreads() {
        return indexThreads;
    }

    public Integer registryLifecycleThreads() {
        return registryLifecycleThreads;
    }

    public Integer busyBackoffMillis() {
        return busyBackoffMillis;
    }

    public Integer busyTimeoutMillis() {
        return busyTimeoutMillis;
    }

    public Boolean backgroundAutoEnabled() {
        return backgroundAutoEnabled;
    }
}
