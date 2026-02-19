package org.hestiastore.monitoring.micrometer;

import java.util.Objects;

import org.hestiastore.index.monitoring.MonitoredIndex;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.monitoring.api.HestiaStoreMetricNames;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;

/**
 * Micrometer binder exposing index operation counters from
 * {@link MonitoredIndex#metricsSnapshot()}.
 */
public final class HestiaStoreMicrometerBinder implements MeterBinder {

    private final MonitoredIndex monitoredIndex;

    /**
     * Creates a binder for one monitored index.
     *
     * @param monitoredIndex source index view
     */
    public HestiaStoreMicrometerBinder(final MonitoredIndex monitoredIndex) {
        this.monitoredIndex = Objects.requireNonNull(monitoredIndex,
                "monitoredIndex");
    }

    /** {@inheritDoc} */
    @Override
    public void bindTo(final MeterRegistry registry) {
        Objects.requireNonNull(registry, "registry");

        FunctionCounter.builder(HestiaStoreMetricNames.OPS_GET_TOTAL,
                monitoredIndex,
                i -> i.metricsSnapshot().getGetOperationCount())
                .description("Total number of get operations")
                .tag("index", monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(HestiaStoreMetricNames.OPS_PUT_TOTAL,
                monitoredIndex,
                i -> i.metricsSnapshot().getPutOperationCount())
                .description("Total number of put operations")
                .tag("index", monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(HestiaStoreMetricNames.OPS_DELETE_TOTAL,
                monitoredIndex,
                i -> i.metricsSnapshot().getDeleteOperationCount())
                .description("Total number of delete operations")
                .tag("index", monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(HestiaStoreMetricNames.REGISTRY_CACHE_HIT_TOTAL,
                monitoredIndex,
                i -> i.metricsSnapshot().getRegistryCacheHitCount())
                .description("Total number of segment registry cache hits")
                .tag("index", monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(HestiaStoreMetricNames.REGISTRY_CACHE_MISS_TOTAL,
                monitoredIndex,
                i -> i.metricsSnapshot().getRegistryCacheMissCount())
                .description("Total number of segment registry cache misses")
                .tag("index", monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(HestiaStoreMetricNames.REGISTRY_CACHE_LOAD_TOTAL,
                monitoredIndex,
                i -> i.metricsSnapshot().getRegistryCacheLoadCount())
                .description("Total number of segment registry cache loads")
                .tag("index", monitoredIndex.indexName()).register(registry);

        FunctionCounter
                .builder(HestiaStoreMetricNames.REGISTRY_CACHE_EVICTION_TOTAL,
                        monitoredIndex,
                        i -> i.metricsSnapshot()
                                .getRegistryCacheEvictionCount())
                .description(
                        "Total number of segment registry cache evictions")
                .tag("index", monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.REGISTRY_CACHE_SIZE,
                monitoredIndex,
                i -> i.metricsSnapshot().getRegistryCacheSize())
                .description("Current segment registry cache size")
                .tag("index", monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.REGISTRY_CACHE_LIMIT,
                monitoredIndex,
                i -> i.metricsSnapshot().getRegistryCacheLimit())
                .description("Configured segment registry cache size limit")
                .tag("index", monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.INDEX_UP,
                monitoredIndex,
                i -> isReady(i.state()) ? 1D : 0D)
                .description("1 when index state is READY, else 0")
                .tag("index", monitoredIndex.indexName()).register(registry);
    }

    private boolean isReady(final SegmentIndexState state) {
        return state == SegmentIndexState.READY;
    }
}
