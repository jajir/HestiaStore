package org.hestiastore.monitoring.micrometer;

/**
 * Metric names exposed by Micrometer integration.
 */
final class HestiaStoreMetricNames {

    static final String OPS_GET_TOTAL = "hestiastore_ops_get_total";
    static final String OPS_PUT_TOTAL = "hestiastore_ops_put_total";
    static final String OPS_DELETE_TOTAL = "hestiastore_ops_delete_total";
    static final String REGISTRY_CACHE_HIT_TOTAL = "hestiastore_registry_cache_hit_total";
    static final String REGISTRY_CACHE_MISS_TOTAL = "hestiastore_registry_cache_miss_total";
    static final String REGISTRY_CACHE_LOAD_TOTAL = "hestiastore_registry_cache_load_total";
    static final String REGISTRY_CACHE_EVICTION_TOTAL = "hestiastore_registry_cache_eviction_total";
    static final String REGISTRY_CACHE_SIZE = "hestiastore_registry_cache_size";
    static final String REGISTRY_CACHE_LIMIT = "hestiastore_registry_cache_limit";
    static final String PARTITION_ACTIVE_LIMIT = "hestiastore_partition_active_limit";
    static final String PARTITION_IMMUTABLE_RUN_LIMIT = "hestiastore_partition_immutable_run_limit";
    static final String PARTITION_BUFFER_LIMIT = "hestiastore_partition_buffer_limit";
    static final String INDEX_BUFFER_LIMIT = "hestiastore_index_buffer_limit";
    static final String PARTITION_COUNT = "hestiastore_partition_count";
    static final String PARTITION_ACTIVE_COUNT = "hestiastore_partition_active_count";
    static final String PARTITION_DRAINING_COUNT = "hestiastore_partition_draining_count";
    static final String PARTITION_IMMUTABLE_RUN_COUNT = "hestiastore_partition_immutable_run_count";
    static final String PARTITION_BUFFERED_KEY_COUNT = "hestiastore_partition_buffered_key_count";
    static final String PARTITION_THROTTLE_LOCAL_TOTAL = "hestiastore_partition_throttle_local_total";
    static final String PARTITION_THROTTLE_GLOBAL_TOTAL = "hestiastore_partition_throttle_global_total";
    static final String PARTITION_DRAIN_SCHEDULE_TOTAL = "hestiastore_partition_drain_schedule_total";
    static final String PARTITION_DRAIN_IN_FLIGHT = "hestiastore_partition_drain_in_flight";
    static final String PARTITION_DRAIN_LATENCY_P95_MICROS = "hestiastore_partition_drain_latency_p95_micros";
    static final String SPLIT_TASK_START_DELAY_P95_MICROS = "hestiastore_split_task_start_delay_p95_micros";
    static final String SPLIT_TASK_RUN_LATENCY_P95_MICROS = "hestiastore_split_task_run_latency_p95_micros";
    static final String DRAIN_TASK_START_DELAY_P95_MICROS = "hestiastore_drain_task_start_delay_p95_micros";
    static final String DRAIN_TASK_RUN_LATENCY_P95_MICROS = "hestiastore_drain_task_run_latency_p95_micros";
    static final String SPLIT_BLOCKED_PARTITION_COUNT = "hestiastore_split_blocked_partition_count";
    static final String SPLIT_BLOCKED_DRAIN_SCHEDULE_TOTAL = "hestiastore_split_blocked_drain_schedule_total";
    static final String BUFFER_FULL_WHILE_SPLIT_BLOCKED_TOTAL = "hestiastore_buffer_full_while_split_blocked_total";
    static final String PUT_BUSY_RETRY_TOTAL = "hestiastore_put_busy_retry_total";
    static final String PUT_BUSY_TIMEOUT_TOTAL = "hestiastore_put_busy_timeout_total";
    static final String PUT_BUSY_WAIT_P95_MICROS = "hestiastore_put_busy_wait_p95_micros";
    static final String FLUSH_ACCEPTED_TO_READY_P95_MICROS = "hestiastore_flush_accepted_to_ready_p95_micros";
    static final String COMPACT_ACCEPTED_TO_READY_P95_MICROS = "hestiastore_compact_accepted_to_ready_p95_micros";
    static final String FLUSH_BUSY_RETRY_TOTAL = "hestiastore_flush_busy_retry_total";
    static final String COMPACT_BUSY_RETRY_TOTAL = "hestiastore_compact_busy_retry_total";
    static final String SPLIT_SCHEDULE_TOTAL = "hestiastore_split_schedule_total";
    static final String SPLIT_IN_FLIGHT = "hestiastore_split_in_flight";
    static final String INDEX_MAINTENANCE_QUEUE_SIZE = "hestiastore_index_maintenance_queue_size";
    static final String INDEX_MAINTENANCE_QUEUE_CAPACITY = "hestiastore_index_maintenance_queue_capacity";
    static final String INDEX_MAINTENANCE_ACTIVE_THREADS = "hestiastore_index_maintenance_active_threads";
    static final String INDEX_MAINTENANCE_COMPLETED_TASKS_TOTAL = "hestiastore_index_maintenance_completed_tasks_total";
    static final String INDEX_MAINTENANCE_REJECTED_TASKS_TOTAL = "hestiastore_index_maintenance_rejected_tasks_total";
    static final String SPLIT_MAINTENANCE_QUEUE_SIZE = "hestiastore_split_maintenance_queue_size";
    static final String SPLIT_MAINTENANCE_QUEUE_CAPACITY = "hestiastore_split_maintenance_queue_capacity";
    static final String SPLIT_MAINTENANCE_ACTIVE_THREADS = "hestiastore_split_maintenance_active_threads";
    static final String SPLIT_MAINTENANCE_COMPLETED_TASKS_TOTAL = "hestiastore_split_maintenance_completed_tasks_total";
    static final String SPLIT_MAINTENANCE_REJECTED_TASKS_TOTAL = "hestiastore_split_maintenance_rejected_tasks_total";
    static final String STABLE_SEGMENT_MAINTENANCE_QUEUE_SIZE = "hestiastore_stable_segment_maintenance_queue_size";
    static final String STABLE_SEGMENT_MAINTENANCE_QUEUE_CAPACITY = "hestiastore_stable_segment_maintenance_queue_capacity";
    static final String STABLE_SEGMENT_MAINTENANCE_ACTIVE_THREADS = "hestiastore_stable_segment_maintenance_active_threads";
    static final String STABLE_SEGMENT_MAINTENANCE_COMPLETED_TASKS_TOTAL = "hestiastore_stable_segment_maintenance_completed_tasks_total";
    static final String STABLE_SEGMENT_MAINTENANCE_CALLER_RUNS_TOTAL = "hestiastore_stable_segment_maintenance_caller_runs_total";
    static final String INDEX_UP = "hestiastore_index_up";

    private HestiaStoreMetricNames() {
    }
}
