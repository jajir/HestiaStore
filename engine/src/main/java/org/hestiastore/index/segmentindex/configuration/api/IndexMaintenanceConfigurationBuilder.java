package org.hestiastore.index.segmentindex.configuration.api;

/**
 * Builder section for maintenance, lifecycle, and retry settings.
 */
public final class IndexMaintenanceConfigurationBuilder {

    private Integer indexThreads;
    private Integer registryLifecycleThreads;
    private Integer busyBackoffMillis;
    private Integer busyTimeoutMillis;
    private Boolean backgroundAutoEnabled;

    IndexMaintenanceConfigurationBuilder() {
    }

    /**
     * Sets index maintenance thread count.
     *
     * @param value index maintenance threads
     * @return this section builder
     */
    public IndexMaintenanceConfigurationBuilder indexThreads(
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
    public IndexMaintenanceConfigurationBuilder registryLifecycleThreads(
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
    public IndexMaintenanceConfigurationBuilder busyBackoffMillis(
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
    public IndexMaintenanceConfigurationBuilder busyTimeoutMillis(
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
    public IndexMaintenanceConfigurationBuilder backgroundAutoEnabled(
            final Boolean value) {
        this.backgroundAutoEnabled = value;
        return this;
    }

    IndexMaintenanceConfiguration build() {
        return new IndexMaintenanceConfiguration(indexThreads,
                registryLifecycleThreads, busyBackoffMillis, busyTimeoutMillis,
                backgroundAutoEnabled);
    }
}
