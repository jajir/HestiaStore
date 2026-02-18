package org.hestiastore.monitoring.micrometer;

import java.util.Objects;

import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.monitoring.api.HestiaStoreMetricNames;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;

/**
 * Micrometer binder exposing index operation counters from
 * {@link SegmentIndex#metricsSnapshot()}.
 */
public final class HestiaStoreMicrometerBinder implements MeterBinder {

    private final SegmentIndex<?, ?> index;
    private final String indexName;

    /**
     * Creates a binder for one index instance.
     *
     * @param index     source index
     * @param indexName logical index name used as metric tag
     */
    public HestiaStoreMicrometerBinder(final SegmentIndex<?, ?> index,
            final String indexName) {
        this.index = Objects.requireNonNull(index, "index");
        this.indexName = Objects.requireNonNull(indexName, "indexName");
    }

    /** {@inheritDoc} */
    @Override
    public void bindTo(final MeterRegistry registry) {
        Objects.requireNonNull(registry, "registry");

        FunctionCounter.builder(HestiaStoreMetricNames.OPS_GET_TOTAL, index,
                i -> i.metricsSnapshot().getGetOperationCount())
                .description("Total number of get operations")
                .tag("index", indexName).register(registry);

        FunctionCounter.builder(HestiaStoreMetricNames.OPS_PUT_TOTAL, index,
                i -> i.metricsSnapshot().getPutOperationCount())
                .description("Total number of put operations")
                .tag("index", indexName).register(registry);

        FunctionCounter.builder(HestiaStoreMetricNames.OPS_DELETE_TOTAL, index,
                i -> i.metricsSnapshot().getDeleteOperationCount())
                .description("Total number of delete operations")
                .tag("index", indexName).register(registry);

        FunctionCounter.builder(HestiaStoreMetricNames.REGISTRY_CACHE_HIT_TOTAL,
                index, i -> i.metricsSnapshot().getRegistryCacheHitCount())
                .description("Total number of segment registry cache hits")
                .tag("index", indexName).register(registry);

        FunctionCounter.builder(HestiaStoreMetricNames.REGISTRY_CACHE_MISS_TOTAL,
                index, i -> i.metricsSnapshot().getRegistryCacheMissCount())
                .description("Total number of segment registry cache misses")
                .tag("index", indexName).register(registry);

        FunctionCounter.builder(HestiaStoreMetricNames.REGISTRY_CACHE_LOAD_TOTAL,
                index, i -> i.metricsSnapshot().getRegistryCacheLoadCount())
                .description("Total number of segment registry cache loads")
                .tag("index", indexName).register(registry);

        FunctionCounter
                .builder(HestiaStoreMetricNames.REGISTRY_CACHE_EVICTION_TOTAL,
                        index,
                        i -> i.metricsSnapshot()
                                .getRegistryCacheEvictionCount())
                .description(
                        "Total number of segment registry cache evictions")
                .tag("index", indexName).register(registry);

        Gauge.builder(HestiaStoreMetricNames.REGISTRY_CACHE_SIZE, index,
                i -> i.metricsSnapshot().getRegistryCacheSize())
                .description("Current segment registry cache size")
                .tag("index", indexName).register(registry);

        Gauge.builder(HestiaStoreMetricNames.REGISTRY_CACHE_LIMIT, index,
                i -> i.metricsSnapshot().getRegistryCacheLimit())
                .description("Configured segment registry cache size limit")
                .tag("index", indexName).register(registry);

        Gauge.builder(HestiaStoreMetricNames.INDEX_UP, index,
                i -> isReady(i.getState()) ? 1D : 0D)
                .description("1 when index state is READY, else 0")
                .tag("index", indexName).register(registry);
    }

    private boolean isReady(final SegmentIndexState state) {
        return state == SegmentIndexState.READY;
    }
}
