package org.hestiastore.index.segmentindex;

import org.hestiastore.index.segmentindex.core.executorregistry.RuntimeExecutorPools;

/**
 * Fluent builder for {@link HestiaStoreRuntime} instances.
 */
public final class HestiaStoreRuntimeBuilder {

    private static final String DEFAULT_THREAD_NAME_PREFIX = "hestia";
    private static final int DEFAULT_SEGMENT_MAINTENANCE_THREADS = 10;
    private static final int DEFAULT_SPLIT_MAINTENANCE_THREADS = 10;
    private static final int DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = 30_000;

    private String threadNamePrefix = DEFAULT_THREAD_NAME_PREFIX;
    private int segmentMaintenanceThreads =
            DEFAULT_SEGMENT_MAINTENANCE_THREADS;
    private int splitMaintenanceThreads = DEFAULT_SPLIT_MAINTENANCE_THREADS;
    private int shutdownTimeoutMillis = DEFAULT_SHUTDOWN_TIMEOUT_MILLIS;

    HestiaStoreRuntimeBuilder() {
    }

    /**
     * Sets the prefix used for runtime-owned thread names.
     *
     * @param value thread name prefix
     * @return this builder
     */
    public HestiaStoreRuntimeBuilder threadNamePrefix(final String value) {
        this.threadNamePrefix = value;
        return this;
    }

    /**
     * Sets the shared segment-maintenance worker count.
     *
     * @param value segment-maintenance worker count
     * @return this builder
     */
    public HestiaStoreRuntimeBuilder segmentMaintenanceThreads(
            final int value) {
        this.segmentMaintenanceThreads = value;
        return this;
    }

    /**
     * Sets the shared split-maintenance worker count.
     *
     * @param value split-maintenance worker count
     * @return this builder
     */
    public HestiaStoreRuntimeBuilder splitMaintenanceThreads(final int value) {
        this.splitMaintenanceThreads = value;
        return this;
    }

    /**
     * Sets the runtime executor shutdown timeout in milliseconds.
     *
     * @param value shutdown timeout in milliseconds
     * @return this builder
     */
    public HestiaStoreRuntimeBuilder shutdownTimeoutMillis(final int value) {
        this.shutdownTimeoutMillis = value;
        return this;
    }

    /**
     * Builds a runtime with the collected settings.
     *
     * @return built runtime
     */
    public HestiaStoreRuntime build() {
        return new HestiaStoreRuntime(threadNamePrefix,
                RuntimeExecutorPools.create(threadNamePrefix,
                        segmentMaintenanceThreads,
                        splitMaintenanceThreads,
                        shutdownTimeoutMillis));
    }
}
