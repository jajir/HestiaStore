package org.hestiastore.index.segmentindex;

/**
 * Builder section for maintenance, lifecycle, and retry settings.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class IndexMaintenanceConfigurationBuilder<K, V> {

    private Integer segmentThreads;
    private Integer indexThreads;
    private Integer registryLifecycleThreads;
    private Integer busyBackoffMillis;
    private Integer busyTimeoutMillis;
    private Boolean backgroundAutoEnabled;

    IndexMaintenanceConfigurationBuilder() {
    }

    /**
     * Sets segment maintenance thread count.
     *
     * @param value segment maintenance threads
     * @return this section builder
     */
    public IndexMaintenanceConfigurationBuilder<K, V> segmentThreads(
            final Integer value) {
        this.segmentThreads = value;
        return this;
    }

    /**
     * Sets index maintenance thread count.
     *
     * @param value index maintenance threads
     * @return this section builder
     */
    public IndexMaintenanceConfigurationBuilder<K, V> indexThreads(
            final Integer value) {
        this.indexThreads = value;
        return this;
    }

    /**
     * Sets registry lifecycle thread count.
     *
     * @param value registry lifecycle threads
     * @return this section builder
     */
    public IndexMaintenanceConfigurationBuilder<K, V> registryLifecycleThreads(
            final Integer value) {
        this.registryLifecycleThreads = value;
        return this;
    }

    /**
     * Sets busy backoff delay in milliseconds.
     *
     * @param value busy backoff delay
     * @return this section builder
     */
    public IndexMaintenanceConfigurationBuilder<K, V> busyBackoffMillis(
            final Integer value) {
        this.busyBackoffMillis = value;
        return this;
    }

    /**
     * Sets busy timeout in milliseconds.
     *
     * @param value busy timeout
     * @return this section builder
     */
    public IndexMaintenanceConfigurationBuilder<K, V> busyTimeoutMillis(
            final Integer value) {
        this.busyTimeoutMillis = value;
        return this;
    }

    /**
     * Sets whether background maintenance is automatically scheduled.
     *
     * @param value true when auto maintenance is enabled
     * @return this section builder
     */
    public IndexMaintenanceConfigurationBuilder<K, V> backgroundAutoEnabled(
            final Boolean value) {
        this.backgroundAutoEnabled = value;
        return this;
    }

    IndexMaintenanceConfiguration build() {
        final Integer effectiveSegmentThreads = segmentThreads == null
                ? IndexConfigurationContract.DEFAULT_SEGMENT_MAINTENANCE_THREADS
                : segmentThreads;
        final Integer effectiveIndexThreads = indexThreads == null
                ? IndexConfigurationContract.DEFAULT_INDEX_MAINTENANCE_THREADS
                : indexThreads;
        final Integer effectiveRegistryLifecycleThreads = registryLifecycleThreads == null
                ? IndexConfigurationContract.DEFAULT_REGISTRY_LIFECYCLE_THREADS
                : registryLifecycleThreads;
        final Integer effectiveBusyBackoffMillis = busyBackoffMillis == null
                ? IndexConfigurationContract.DEFAULT_INDEX_BUSY_BACKOFF_MILLIS
                : busyBackoffMillis;
        final Integer effectiveBusyTimeoutMillis = busyTimeoutMillis == null
                ? IndexConfigurationContract.DEFAULT_INDEX_BUSY_TIMEOUT_MILLIS
                : busyTimeoutMillis;
        final Boolean effectiveBackgroundAutoEnabled = backgroundAutoEnabled == null
                ? IndexConfigurationContract.DEFAULT_BACKGROUND_MAINTENANCE_AUTO_ENABLED
                : backgroundAutoEnabled;
        return new IndexMaintenanceConfiguration(effectiveSegmentThreads,
                effectiveIndexThreads, effectiveRegistryLifecycleThreads,
                effectiveBusyBackoffMillis, effectiveBusyTimeoutMillis,
                effectiveBackgroundAutoEnabled);
    }
}
