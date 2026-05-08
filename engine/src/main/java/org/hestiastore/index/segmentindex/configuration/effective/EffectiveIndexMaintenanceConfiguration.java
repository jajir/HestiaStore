package org.hestiastore.index.segmentindex.configuration.effective;

import org.hestiastore.index.Vldtn;

/**
 * Resolved maintenance, lifecycle, and retry settings.
 */
public final class EffectiveIndexMaintenanceConfiguration {

    private final int segmentThreads;
    private final int indexThreads;
    private final int registryLifecycleThreads;
    private final int busyBackoffMillis;
    private final int busyTimeoutMillis;
    private final boolean backgroundAutoEnabled;

    public EffectiveIndexMaintenanceConfiguration(final int segmentThreads,
            final int indexThreads, final int registryLifecycleThreads,
            final int busyBackoffMillis, final int busyTimeoutMillis,
            final boolean backgroundAutoEnabled) {
        this.segmentThreads = Vldtn.requireGreaterThanZero(segmentThreads,
                "segmentThreads");
        this.indexThreads = Vldtn.requireGreaterThanZero(indexThreads,
                "indexThreads");
        this.registryLifecycleThreads = Vldtn.requireGreaterThanZero(
                registryLifecycleThreads, "registryLifecycleThreads");
        this.busyBackoffMillis = Vldtn.requireGreaterThanZero(
                busyBackoffMillis, "busyBackoffMillis");
        this.busyTimeoutMillis = Vldtn.requireGreaterThanZero(
                busyTimeoutMillis, "busyTimeoutMillis");
        this.backgroundAutoEnabled = backgroundAutoEnabled;
    }

    public int segmentThreads() {
        return segmentThreads;
    }

    public int indexThreads() {
        return indexThreads;
    }

    public int registryLifecycleThreads() {
        return registryLifecycleThreads;
    }

    public int busyBackoffMillis() {
        return busyBackoffMillis;
    }

    public int busyTimeoutMillis() {
        return busyTimeoutMillis;
    }

    public boolean backgroundAutoEnabled() {
        return backgroundAutoEnabled;
    }
}
