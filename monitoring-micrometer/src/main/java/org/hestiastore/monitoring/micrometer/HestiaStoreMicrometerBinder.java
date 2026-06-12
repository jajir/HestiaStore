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
 * {@link MonitoredIndex#runtimeSnapshot()}.
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
                i -> i.runtimeSnapshot().operations().readOperationCount())
                .description("Total number of get operations")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(HestiaStoreMetricNames.OPS_PUT_TOTAL,
                monitoredIndex,
                i -> i.runtimeSnapshot().operations().putOperationCount())
                .description("Total number of put operations")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(HestiaStoreMetricNames.OPS_DELETE_TOTAL,
                monitoredIndex,
                i -> i.runtimeSnapshot().operations().deleteOperationCount())
                .description("Total number of delete operations")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(HestiaStoreMetricNames.REGISTRY_CACHE_HIT_TOTAL,
                monitoredIndex,
                i -> i.runtimeSnapshot().registryCache().hitCount())
                .description("Total number of segment registry cache hits")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(HestiaStoreMetricNames.REGISTRY_CACHE_MISS_TOTAL,
                monitoredIndex,
                i -> i.runtimeSnapshot().registryCache().missCount())
                .description("Total number of segment registry cache misses")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(HestiaStoreMetricNames.REGISTRY_CACHE_LOAD_TOTAL,
                monitoredIndex,
                i -> i.runtimeSnapshot().registryCache().loadCount())
                .description("Total number of segment registry cache loads")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter
                .builder(HestiaStoreMetricNames.REGISTRY_CACHE_EVICTION_TOTAL,
                        monitoredIndex,
                        i -> i.runtimeSnapshot()
                                .registryCache().evictionCount())
                .description(
                        "Total number of segment registry cache evictions")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.REGISTRY_CACHE_SIZE,
                monitoredIndex,
                i -> i.runtimeSnapshot().registryCache().size())
                .description("Current segment registry cache size")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.REGISTRY_CACHE_LIMIT,
                monitoredIndex,
                i -> i.runtimeSnapshot().registryCache().limit())
                .description("Configured segment registry cache size limit")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.CHUNK_STORE_CACHE_PAGE_LIMIT,
                monitoredIndex,
                i -> i.runtimeSnapshot().chunkStoreCache().pageLimit())
                .description("Configured parsed chunk page cache limit")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.CHUNK_STORE_CACHE_PAGE_COUNT,
                monitoredIndex,
                i -> i.runtimeSnapshot().chunkStoreCache().pageCount())
                .description("Current parsed chunk page cache page count")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.CHUNK_STORE_CACHE_ENTRY_COUNT,
                monitoredIndex,
                i -> i.runtimeSnapshot().chunkStoreCache().entryCount())
                .description("Current parsed chunk page cache entry count")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(
                HestiaStoreMetricNames.CHUNK_STORE_CACHE_HIT_TOTAL,
                monitoredIndex,
                i -> i.runtimeSnapshot().chunkStoreCache().hitCount())
                .description("Total number of parsed chunk page cache hits")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(
                HestiaStoreMetricNames.CHUNK_STORE_CACHE_MISS_TOTAL,
                monitoredIndex,
                i -> i.runtimeSnapshot().chunkStoreCache().missCount())
                .description("Total number of parsed chunk page cache misses")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(
                HestiaStoreMetricNames.CHUNK_STORE_CACHE_LOAD_TOTAL,
                monitoredIndex,
                i -> i.runtimeSnapshot().chunkStoreCache().loadCount())
                .description("Total number of parsed chunk page cache loads")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(
                HestiaStoreMetricNames.CHUNK_STORE_CACHE_EVICTION_TOTAL,
                monitoredIndex,
                i -> i.runtimeSnapshot().chunkStoreCache().evictionCount())
                .description(
                        "Total number of parsed chunk page cache evictions")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(
                HestiaStoreMetricNames.CHUNK_STORE_CACHE_INVALIDATION_TOTAL,
                monitoredIndex,
                i -> i.runtimeSnapshot()
                        .chunkStoreCache().invalidationCount())
                .description(
                        "Total number of parsed chunk page cache invalidations")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.SEGMENT_WRITE_CACHE_KEY_LIMIT,
                monitoredIndex,
                i -> i.runtimeSnapshot().writePath()
                        .segmentWriteCacheKeyLimit())
                .description("Configured segment write-cache key limit")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(
                HestiaStoreMetricNames.SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE,
                monitoredIndex,
                i -> i.runtimeSnapshot()
                        .writePath()
                        .segmentWriteCacheKeyLimitDuringMaintenance())
                .description(
                        "Configured maintenance-time segment write-cache key limit")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.INDEX_BUFFERED_WRITE_KEY_LIMIT,
                monitoredIndex,
                i -> i.runtimeSnapshot().writePath()
                        .indexBufferedWriteKeyLimit())
                .description("Configured index-wide buffered write key limit")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.SPLIT_TASK_START_DELAY_P95_MICROS,
                monitoredIndex,
                i -> i.runtimeSnapshot().split().taskStartDelayP95Micros())
                .description("Observed P95 split task queue delay in microseconds")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.SPLIT_TASK_RUN_LATENCY_P95_MICROS,
                monitoredIndex,
                i -> i.runtimeSnapshot().split().taskRunLatencyP95Micros())
                .description("Observed P95 split task run latency in microseconds")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.FLUSH_ACCEPTED_TO_READY_P95_MICROS,
                monitoredIndex,
                i -> i.runtimeSnapshot().maintenance()
                        .flushAcceptedToReadyP95Micros())
                .description("Observed P95 flush accepted-to-ready latency in microseconds")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(
                HestiaStoreMetricNames.COMPACT_ACCEPTED_TO_READY_P95_MICROS,
                monitoredIndex, i -> i.runtimeSnapshot()
                        .maintenance().compactAcceptedToReadyP95Micros())
                .description(
                        "Observed P95 compact accepted-to-ready latency in microseconds")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(HestiaStoreMetricNames.FLUSH_BUSY_RETRY_TOTAL,
                monitoredIndex,
                i -> i.runtimeSnapshot().maintenance().flushBusyRetryCount())
                .description("Total number of BUSY retries observed by flush operations")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(
                HestiaStoreMetricNames.COMPACT_BUSY_RETRY_TOTAL,
                monitoredIndex,
                i -> i.runtimeSnapshot().maintenance()
                        .compactBusyRetryCount())
                .description("Total number of BUSY retries observed by compact operations")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(HestiaStoreMetricNames.SPLIT_SCHEDULE_TOTAL,
                monitoredIndex,
                i -> i.runtimeSnapshot().split().scheduleCount())
                .description("Total number of scheduled split operations")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.SPLIT_IN_FLIGHT,
                monitoredIndex,
                i -> i.runtimeSnapshot().split().inFlightCount())
                .description("Current number of in-flight split operations")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.INDEX_MAINTENANCE_QUEUE_SIZE,
                monitoredIndex,
                i -> i.runtimeSnapshot().maintenance()
                        .indexExecutor().queueSize())
                .description("Current index-maintenance executor queue size")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.INDEX_MAINTENANCE_QUEUE_CAPACITY,
                monitoredIndex,
                i -> i.runtimeSnapshot().maintenance()
                        .indexExecutor().queueCapacity())
                .description("Configured index-maintenance executor queue capacity")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.INDEX_MAINTENANCE_ACTIVE_THREADS,
                monitoredIndex,
                i -> i.runtimeSnapshot().maintenance()
                        .indexExecutor().activeThreadCount())
                .description("Current number of active index-maintenance threads")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(
                HestiaStoreMetricNames.INDEX_MAINTENANCE_COMPLETED_TASKS_TOTAL,
                monitoredIndex, i -> i.runtimeSnapshot()
                        .maintenance().indexExecutor().completedTaskCount())
                .description(
                        "Total number of completed index-maintenance tasks")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(
                HestiaStoreMetricNames.INDEX_MAINTENANCE_REJECTED_TASKS_TOTAL,
                monitoredIndex, i -> i.runtimeSnapshot()
                        .maintenance().indexExecutor().rejectedTaskCount())
                .description(
                        "Total number of rejected index-maintenance tasks")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.SPLIT_MAINTENANCE_QUEUE_SIZE,
                monitoredIndex,
                i -> i.runtimeSnapshot().split().executor().queueSize())
                .description("Current split-maintenance executor queue size")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.SPLIT_MAINTENANCE_QUEUE_CAPACITY,
                monitoredIndex,
                i -> i.runtimeSnapshot().split().executor().queueCapacity())
                .description("Configured split-maintenance executor queue capacity")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.SPLIT_MAINTENANCE_ACTIVE_THREADS,
                monitoredIndex,
                i -> i.runtimeSnapshot().split().executor()
                        .activeThreadCount())
                .description("Current number of active split-maintenance threads")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(
                HestiaStoreMetricNames.SPLIT_MAINTENANCE_COMPLETED_TASKS_TOTAL,
                monitoredIndex, i -> i.runtimeSnapshot()
                        .split().executor().completedTaskCount())
                .description("Total number of completed split-maintenance tasks")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(
                HestiaStoreMetricNames.SPLIT_MAINTENANCE_REJECTED_TASKS_TOTAL,
                monitoredIndex, i -> i.runtimeSnapshot()
                        .split().executor().rejectedTaskCount())
                .description("Total number of rejected split-maintenance tasks")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(
                HestiaStoreMetricNames.STABLE_SEGMENT_MAINTENANCE_QUEUE_SIZE,
                monitoredIndex, i -> i.runtimeSnapshot()
                        .maintenance().stableSegmentExecutor().queueSize())
                .description(
                        "Current stable-segment maintenance executor queue size")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(
                HestiaStoreMetricNames.STABLE_SEGMENT_MAINTENANCE_QUEUE_CAPACITY,
                monitoredIndex, i -> i.runtimeSnapshot()
                        .maintenance().stableSegmentExecutor().queueCapacity())
                .description(
                        "Configured stable-segment maintenance executor queue capacity")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(
                HestiaStoreMetricNames.STABLE_SEGMENT_MAINTENANCE_ACTIVE_THREADS,
                monitoredIndex, i -> i.runtimeSnapshot()
                        .maintenance().stableSegmentExecutor()
                        .activeThreadCount())
                .description(
                        "Current number of active stable-segment maintenance threads")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(
                HestiaStoreMetricNames.STABLE_SEGMENT_MAINTENANCE_COMPLETED_TASKS_TOTAL,
                monitoredIndex, i -> i.runtimeSnapshot()
                        .maintenance().stableSegmentExecutor()
                        .completedTaskCount())
                .description(
                        "Total number of completed stable-segment maintenance tasks")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(
                HestiaStoreMetricNames.STABLE_SEGMENT_MAINTENANCE_CALLER_RUNS_TOTAL,
                monitoredIndex, i -> i.runtimeSnapshot()
                        .maintenance().stableSegmentExecutor()
                        .callerRunsCount())
                .description(
                        "Total number of stable-segment maintenance tasks executed on caller threads")
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
