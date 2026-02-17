package org.hestiastore.monitoring.prometheus;

import org.hestiastore.index.segmentindex.SegmentIndex;
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
     * @param index source index
     * @param indexName index tag value
     * @return registry ready for scraping
     */
    public static PrometheusMeterRegistry createRegistry(
            final SegmentIndex<?, ?> index, final String indexName) {
        final PrometheusMeterRegistry registry = new PrometheusMeterRegistry(
                PrometheusConfig.DEFAULT);
        new HestiaStoreMicrometerBinder(index, indexName).bindTo(registry);
        return registry;
    }

    /**
     * Creates a scrape payload for provided index.
     *
     * @param index source index
     * @param indexName index tag value
     * @return prometheus text exposition
     */
    public static String scrape(final SegmentIndex<?, ?> index,
            final String indexName) {
        return createRegistry(index, indexName).scrape();
    }
}
