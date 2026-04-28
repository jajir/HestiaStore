package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Vldtn;

/**
 * Builder section for maintenance, lifecycle, and retry settings.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class IndexMaintenanceConfigurationBuilder<K, V> {

    private final IndexConfigurationBuilder<K, V> builder;

    IndexMaintenanceConfigurationBuilder(
            final IndexConfigurationBuilder<K, V> builder) {
        this.builder = Vldtn.requireNonNull(builder, "builder");
    }

    /**
     * Sets segment maintenance thread count.
     *
     * @param value segment maintenance threads
     * @return this section builder
     */
    public IndexMaintenanceConfigurationBuilder<K, V> segmentThreads(
            final Integer value) {
        builder.setSegmentMaintenanceThreadCount(value);
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
        builder.setIndexMaintenanceThreadCount(value);
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
        builder.setRegistryLifecycleThreadCount(value);
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
        builder.setBusyBackoffMillis(value);
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
        builder.setBusyTimeoutMillis(value);
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
        builder.setBackgroundMaintenanceAutoEnabled(value);
        return this;
    }
}
