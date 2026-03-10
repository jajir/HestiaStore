package org.hestiastore.monitoring.micrometer;

import java.util.Objects;

import org.hestiastore.index.monitoring.MonitoredIndex;
import org.hestiastore.index.segmentindex.SegmentIndexState;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;

/**
 * Micrometer binder exposing index operation counters from
 * {@link MonitoredIndex#metricsSnapshot()}.
 */
public final class HestiaStoreMicrometerBinder implements MeterBinder {

    private static final String TAG_INDEX = "index";
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
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(HestiaStoreMetricNames.OPS_PUT_TOTAL,
                monitoredIndex,
                i -> i.metricsSnapshot().getPutOperationCount())
                .description("Total number of put operations")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(HestiaStoreMetricNames.OPS_DELETE_TOTAL,
                monitoredIndex,
                i -> i.metricsSnapshot().getDeleteOperationCount())
                .description("Total number of delete operations")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(HestiaStoreMetricNames.REGISTRY_CACHE_HIT_TOTAL,
                monitoredIndex,
                i -> i.metricsSnapshot().getRegistryCacheHitCount())
                .description("Total number of segment registry cache hits")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(HestiaStoreMetricNames.REGISTRY_CACHE_MISS_TOTAL,
                monitoredIndex,
                i -> i.metricsSnapshot().getRegistryCacheMissCount())
                .description("Total number of segment registry cache misses")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(HestiaStoreMetricNames.REGISTRY_CACHE_LOAD_TOTAL,
                monitoredIndex,
                i -> i.metricsSnapshot().getRegistryCacheLoadCount())
                .description("Total number of segment registry cache loads")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter
                .builder(HestiaStoreMetricNames.REGISTRY_CACHE_EVICTION_TOTAL,
                        monitoredIndex,
                        i -> i.metricsSnapshot()
                                .getRegistryCacheEvictionCount())
                .description(
                        "Total number of segment registry cache evictions")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.REGISTRY_CACHE_SIZE,
                monitoredIndex,
                i -> i.metricsSnapshot().getRegistryCacheSize())
                .description("Current segment registry cache size")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.REGISTRY_CACHE_LIMIT,
                monitoredIndex,
                i -> i.metricsSnapshot().getRegistryCacheLimit())
                .description("Configured segment registry cache size limit")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.PARTITION_ACTIVE_LIMIT,
                monitoredIndex,
                i -> i.metricsSnapshot().getMaxNumberOfKeysInActivePartition())
                .description("Configured active partition key limit")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.PARTITION_IMMUTABLE_RUN_LIMIT,
                monitoredIndex,
                i -> i.metricsSnapshot()
                        .getMaxNumberOfImmutableRunsPerPartition())
                .description("Configured immutable run limit per partition")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.PARTITION_BUFFER_LIMIT,
                monitoredIndex,
                i -> i.metricsSnapshot().getMaxNumberOfKeysInPartitionBuffer())
                .description("Configured per-partition buffered key limit")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.INDEX_BUFFER_LIMIT,
                monitoredIndex,
                i -> i.metricsSnapshot().getMaxNumberOfKeysInIndexBuffer())
                .description("Configured index-wide buffered key limit")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.PARTITION_COUNT, monitoredIndex,
                i -> i.metricsSnapshot().getPartitionCount())
                .description("Current number of routed partitions")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.PARTITION_ACTIVE_COUNT,
                monitoredIndex,
                i -> i.metricsSnapshot().getActivePartitionCount())
                .description("Current number of partitions with active overlay data")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.PARTITION_DRAINING_COUNT,
                monitoredIndex,
                i -> i.metricsSnapshot().getDrainingPartitionCount())
                .description("Current number of partitions draining to stable storage")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.PARTITION_IMMUTABLE_RUN_COUNT,
                monitoredIndex,
                i -> i.metricsSnapshot().getImmutableRunCount())
                .description("Current number of immutable overlay runs")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.PARTITION_BUFFERED_KEY_COUNT,
                monitoredIndex,
                i -> i.metricsSnapshot().getPartitionBufferedKeyCount())
                .description("Current number of keys buffered in partition overlays")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(
                HestiaStoreMetricNames.PARTITION_THROTTLE_LOCAL_TOTAL,
                monitoredIndex,
                i -> i.metricsSnapshot().getLocalThrottleCount())
                .description("Total number of local partition throttle events")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(
                HestiaStoreMetricNames.PARTITION_THROTTLE_GLOBAL_TOTAL,
                monitoredIndex,
                i -> i.metricsSnapshot().getGlobalThrottleCount())
                .description("Total number of global overlay throttle events")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(
                HestiaStoreMetricNames.PARTITION_DRAIN_SCHEDULE_TOTAL,
                monitoredIndex,
                i -> i.metricsSnapshot().getDrainScheduleCount())
                .description("Total number of scheduled partition drains")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.PARTITION_DRAIN_IN_FLIGHT,
                monitoredIndex,
                i -> i.metricsSnapshot().getDrainInFlightCount())
                .description("Current number of in-flight partition drains")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.INDEX_UP,
                monitoredIndex,
                i -> isReady(i.state()) ? 1D : 0D)
                .description("1 when index state is READY, else 0")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);
    }

    private boolean isReady(final SegmentIndexState state) {
        return state == SegmentIndexState.READY;
    }
}
