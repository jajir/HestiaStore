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

        Gauge.builder(HestiaStoreMetricNames.PARTITION_DRAIN_LATENCY_P95_MICROS,
                monitoredIndex,
                i -> i.metricsSnapshot().getDrainLatencyP95Micros())
                .description("Observed P95 partition drain latency in microseconds")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.SPLIT_TASK_START_DELAY_P95_MICROS,
                monitoredIndex,
                i -> i.metricsSnapshot().getSplitTaskStartDelayP95Micros())
                .description("Observed P95 split task queue delay in microseconds")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.SPLIT_TASK_RUN_LATENCY_P95_MICROS,
                monitoredIndex,
                i -> i.metricsSnapshot().getSplitTaskRunLatencyP95Micros())
                .description("Observed P95 split task run latency in microseconds")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.DRAIN_TASK_START_DELAY_P95_MICROS,
                monitoredIndex,
                i -> i.metricsSnapshot().getDrainTaskStartDelayP95Micros())
                .description("Observed P95 drain task queue delay in microseconds")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.DRAIN_TASK_RUN_LATENCY_P95_MICROS,
                monitoredIndex,
                i -> i.metricsSnapshot().getDrainTaskRunLatencyP95Micros())
                .description("Observed P95 drain task run latency in microseconds")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.SPLIT_BLOCKED_PARTITION_COUNT,
                monitoredIndex,
                i -> i.metricsSnapshot().getSplitBlockedPartitionCount())
                .description("Current number of partitions with split-blocked drain scheduling")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(
                HestiaStoreMetricNames.SPLIT_BLOCKED_DRAIN_SCHEDULE_TOTAL,
                monitoredIndex,
                i -> i.metricsSnapshot().getSplitBlockedDrainScheduleCount())
                .description("Total number of drain schedule attempts blocked by active splits")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(
                HestiaStoreMetricNames.BUFFER_FULL_WHILE_SPLIT_BLOCKED_TOTAL,
                monitoredIndex, i -> i.metricsSnapshot()
                        .getBufferFullWhileSplitBlockedCount())
                .description(
                        "Total number of BUSY writes observed while split blocked drain scheduling")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(HestiaStoreMetricNames.PUT_BUSY_RETRY_TOTAL,
                monitoredIndex,
                i -> i.metricsSnapshot().getPutBusyRetryCount())
                .description("Total number of BUSY retries observed by put operations")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(HestiaStoreMetricNames.PUT_BUSY_TIMEOUT_TOTAL,
                monitoredIndex,
                i -> i.metricsSnapshot().getPutBusyTimeoutCount())
                .description("Total number of put operations that timed out while retrying BUSY")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.PUT_BUSY_WAIT_P95_MICROS,
                monitoredIndex,
                i -> i.metricsSnapshot().getPutBusyWaitP95Micros())
                .description("Observed P95 put BUSY wait time in microseconds")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.FLUSH_ACCEPTED_TO_READY_P95_MICROS,
                monitoredIndex,
                i -> i.metricsSnapshot().getFlushAcceptedToReadyP95Micros())
                .description("Observed P95 flush accepted-to-ready latency in microseconds")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(
                HestiaStoreMetricNames.COMPACT_ACCEPTED_TO_READY_P95_MICROS,
                monitoredIndex, i -> i.metricsSnapshot()
                        .getCompactAcceptedToReadyP95Micros())
                .description(
                        "Observed P95 compact accepted-to-ready latency in microseconds")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(HestiaStoreMetricNames.FLUSH_BUSY_RETRY_TOTAL,
                monitoredIndex,
                i -> i.metricsSnapshot().getFlushBusyRetryCount())
                .description("Total number of BUSY retries observed by flush operations")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(
                HestiaStoreMetricNames.COMPACT_BUSY_RETRY_TOTAL,
                monitoredIndex,
                i -> i.metricsSnapshot().getCompactBusyRetryCount())
                .description("Total number of BUSY retries observed by compact operations")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(HestiaStoreMetricNames.SPLIT_SCHEDULE_TOTAL,
                monitoredIndex,
                i -> i.metricsSnapshot().getSplitScheduleCount())
                .description("Total number of scheduled split operations")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.SPLIT_IN_FLIGHT,
                monitoredIndex,
                i -> i.metricsSnapshot().getSplitInFlightCount())
                .description("Current number of in-flight split operations")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.INDEX_MAINTENANCE_QUEUE_SIZE,
                monitoredIndex,
                i -> i.metricsSnapshot().getMaintenanceQueueSize())
                .description("Current index-maintenance executor queue size")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.INDEX_MAINTENANCE_QUEUE_CAPACITY,
                monitoredIndex,
                i -> i.metricsSnapshot().getMaintenanceQueueCapacity())
                .description("Configured index-maintenance executor queue capacity")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.INDEX_MAINTENANCE_ACTIVE_THREADS,
                monitoredIndex,
                i -> i.metricsSnapshot().getIndexMaintenanceActiveThreadCount())
                .description("Current number of active index-maintenance threads")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(
                HestiaStoreMetricNames.INDEX_MAINTENANCE_COMPLETED_TASKS_TOTAL,
                monitoredIndex, i -> i.metricsSnapshot()
                        .getIndexMaintenanceCompletedTaskCount())
                .description(
                        "Total number of completed index-maintenance tasks")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(
                HestiaStoreMetricNames.INDEX_MAINTENANCE_REJECTED_TASKS_TOTAL,
                monitoredIndex, i -> i.metricsSnapshot()
                        .getIndexMaintenanceRejectedTaskCount())
                .description(
                        "Total number of rejected index-maintenance tasks")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.SPLIT_MAINTENANCE_QUEUE_SIZE,
                monitoredIndex, i -> i.metricsSnapshot().getSplitQueueSize())
                .description("Current split-maintenance executor queue size")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.SPLIT_MAINTENANCE_QUEUE_CAPACITY,
                monitoredIndex,
                i -> i.metricsSnapshot().getSplitQueueCapacity())
                .description("Configured split-maintenance executor queue capacity")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(HestiaStoreMetricNames.SPLIT_MAINTENANCE_ACTIVE_THREADS,
                monitoredIndex,
                i -> i.metricsSnapshot().getSplitMaintenanceActiveThreadCount())
                .description("Current number of active split-maintenance threads")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(
                HestiaStoreMetricNames.SPLIT_MAINTENANCE_COMPLETED_TASKS_TOTAL,
                monitoredIndex, i -> i.metricsSnapshot()
                        .getSplitMaintenanceCompletedTaskCount())
                .description("Total number of completed split-maintenance tasks")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(
                HestiaStoreMetricNames.SPLIT_MAINTENANCE_REJECTED_TASKS_TOTAL,
                monitoredIndex, i -> i.metricsSnapshot()
                        .getSplitMaintenanceRejectedTaskCount())
                .description("Total number of rejected split-maintenance tasks")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(
                HestiaStoreMetricNames.STABLE_SEGMENT_MAINTENANCE_QUEUE_SIZE,
                monitoredIndex, i -> i.metricsSnapshot()
                        .getStableSegmentMaintenanceQueueSize())
                .description(
                        "Current stable-segment maintenance executor queue size")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(
                HestiaStoreMetricNames.STABLE_SEGMENT_MAINTENANCE_QUEUE_CAPACITY,
                monitoredIndex, i -> i.metricsSnapshot()
                        .getStableSegmentMaintenanceQueueCapacity())
                .description(
                        "Configured stable-segment maintenance executor queue capacity")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        Gauge.builder(
                HestiaStoreMetricNames.STABLE_SEGMENT_MAINTENANCE_ACTIVE_THREADS,
                monitoredIndex, i -> i.metricsSnapshot()
                        .getStableSegmentMaintenanceActiveThreadCount())
                .description(
                        "Current number of active stable-segment maintenance threads")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(
                HestiaStoreMetricNames.STABLE_SEGMENT_MAINTENANCE_COMPLETED_TASKS_TOTAL,
                monitoredIndex, i -> i.metricsSnapshot()
                        .getStableSegmentMaintenanceCompletedTaskCount())
                .description(
                        "Total number of completed stable-segment maintenance tasks")
                .tag(TAG_INDEX, monitoredIndex.indexName()).register(registry);

        FunctionCounter.builder(
                HestiaStoreMetricNames.STABLE_SEGMENT_MAINTENANCE_CALLER_RUNS_TOTAL,
                monitoredIndex, i -> i.metricsSnapshot()
                        .getStableSegmentMaintenanceCallerRunsCount())
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
