package org.hestiastore.monitoring.prometheus;

import org.hestiastore.index.monitoring.MonitoredIndex;
import org.hestiastore.monitoring.micrometer.HestiaStoreMicrometerBinder;

import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

/**
 * Helper for exposing HestiaStore metrics in Prometheus text format.
 */
public final class HestiaStorePrometheusExporter {

    private HestiaStorePrometheusExporter() {
    }

    /**
     * Creates Prometheus registry and binds HestiaStore metrics.
     *
     * @param monitoredIndex monitored index source
     * @return registry ready for scraping
     */
    public static PrometheusMeterRegistry createRegistry(
            final MonitoredIndex monitoredIndex) {
        final PrometheusMeterRegistry registry = new PrometheusMeterRegistry(
                PrometheusConfig.DEFAULT);
        new HestiaStoreMicrometerBinder(monitoredIndex).bindTo(registry);
        return registry;
    }

    /**
     * Creates a scrape payload for provided index.
     *
     * @param monitoredIndex monitored index source
     * @return prometheus text exposition
     */
    public static String scrape(final MonitoredIndex monitoredIndex) {
        return createRegistry(monitoredIndex).scrape();
    }
}
