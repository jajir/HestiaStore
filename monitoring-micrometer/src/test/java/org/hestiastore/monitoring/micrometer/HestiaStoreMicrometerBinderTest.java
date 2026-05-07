package org.hestiastore.monitoring.micrometer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeSnapshot;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

class HestiaStoreMicrometerBinderTest {

    @SuppressWarnings("unchecked")
    @Test
    void bindTo_exposesDynamicCountersAndStateGauge() {
        final SegmentIndex<Integer, String> index = mock(SegmentIndex.class);
        final IndexRuntimeMonitoring runtimeMonitoring = mock(
                IndexRuntimeMonitoring.class);
        final AtomicReference<SegmentIndexMetricsSnapshot> snapshotRef = new AtomicReference<>(
                snapshot(1L, 2L, 3L, SegmentIndexState.READY, 7, 11, 29,
                        37L, 2, 50));
        when(index.runtimeMonitoring()).thenReturn(runtimeMonitoring);
        when(runtimeMonitoring.snapshot()).thenAnswer(inv -> new IndexRuntimeSnapshot(
                "orders", snapshotRef.get().getState(), snapshotRef.get(),
                Instant.now()));

        final SimpleMeterRegistry registry = new SimpleMeterRegistry();
        new HestiaStoreMicrometerBinder(
                new MicrometerSegmentIndexSource("orders", index))
                        .bindTo(registry);

        assertMetrics(registry, 1D, 2D, 3D, 7D, 11D, 29D, 37D, 2D, 1D, 50);

        snapshotRef.set(snapshot(5L, 8L, 13L, SegmentIndexState.CLOSED, 9, 13,
                15, 41L, 0, 70));

        assertMetrics(registry, 5D, 8D, 13D, 9D, 13D, 15D, 41D, 0D, 0D, 70);
    }

    private SegmentIndexMetricsSnapshot snapshot(final long getCount,
            final long putCount, final long deleteCount,
            final SegmentIndexState state, final int segmentWriteCacheKeyLimit,
            final int segmentWriteCacheKeyLimitDuringMaintenance,
            final int indexBufferedWriteKeyLimit,
            final long splitScheduleCount, final int splitInFlightCount,
            final int executorBase) {
        return new SegmentIndexMetricsSnapshot(
                getCount, putCount, deleteCount,
                0L, 0L, 0L, 0L,
                0, 0,
                0, segmentWriteCacheKeyLimit,
                segmentWriteCacheKeyLimitDuringMaintenance,
                indexBufferedWriteKeyLimit, 0, 0, 0, 0, 0, 0,
                0L, 0L, 0L, 0L,
                0L, 0L, splitScheduleCount,
                splitInFlightCount, executorBase, executorBase + 1,
                executorBase + 5, executorBase + 6, executorBase + 2,
                executorBase + 3L, executorBase + 4L, executorBase + 7,
                executorBase + 8L, executorBase + 9L, executorBase + 12,
                executorBase + 10, executorBase + 11, executorBase + 13L,
                executorBase + 14L,
                0L, 0L, 0L,
                0L, 0L, 0L,
                0, 0, 0D,
                0L, 0L, 0L, 0L,
                false, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                0, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                executorBase + 15L,
                executorBase + 16L, executorBase + 17L, executorBase + 18L,
                executorBase + 19, executorBase + 20L, executorBase + 21L,
                executorBase + 22L, executorBase + 23L, executorBase + 24L,
                executorBase + 25L, List.of(), state);
    }

    private static void assertMetrics(final SimpleMeterRegistry registry,
            final double getCount, final double putCount,
            final double deleteCount,
            final double segmentWriteCacheKeyLimit,
            final double segmentWriteCacheKeyLimitDuringMaintenance,
            final double indexBufferedWriteKeyLimit,
            final double splitScheduleCount, final double splitInFlightCount,
            final double indexUp, final int executorBase) {
        assertFunctionCounter(registry, "hestiastore_ops_get_total", getCount);
        assertFunctionCounter(registry, "hestiastore_ops_put_total", putCount);
        assertFunctionCounter(registry, "hestiastore_ops_delete_total",
                deleteCount);
        assertGauge(registry, "hestiastore_segment_write_cache_key_limit",
                segmentWriteCacheKeyLimit);
        assertGauge(registry,
                "hestiastore_segment_write_cache_key_limit_during_maintenance",
                segmentWriteCacheKeyLimitDuringMaintenance);
        assertGauge(registry, "hestiastore_index_buffered_write_key_limit",
                indexBufferedWriteKeyLimit);
        assertGauge(registry, "hestiastore_split_task_start_delay_p95_micros",
                executorBase + 15D);
        assertGauge(registry, "hestiastore_split_task_run_latency_p95_micros",
                executorBase + 16D);
        assertGauge(registry, "hestiastore_drain_task_start_delay_p95_micros",
                executorBase + 17D);
        assertGauge(registry, "hestiastore_drain_task_run_latency_p95_micros",
                executorBase + 18D);
        assertFunctionCounter(registry, "hestiastore_put_busy_retry_total",
                executorBase + 19D);
        assertFunctionCounter(registry, "hestiastore_put_busy_timeout_total",
                executorBase + 20D);
        assertGauge(registry, "hestiastore_put_busy_wait_p95_micros",
                executorBase + 21D);
        assertGauge(registry,
                "hestiastore_flush_accepted_to_ready_p95_micros",
                executorBase + 22D);
        assertGauge(registry,
                "hestiastore_compact_accepted_to_ready_p95_micros",
                executorBase + 23D);
        assertFunctionCounter(registry, "hestiastore_flush_busy_retry_total",
                executorBase + 24D);
        assertFunctionCounter(registry,
                "hestiastore_compact_busy_retry_total", executorBase + 25D);
        assertFunctionCounter(registry, "hestiastore_split_schedule_total",
                splitScheduleCount);
        assertGauge(registry, "hestiastore_split_in_flight", splitInFlightCount);
        assertGauge(registry, "hestiastore_index_maintenance_queue_size",
                executorBase);
        assertGauge(registry, "hestiastore_index_maintenance_queue_capacity",
                executorBase + 1D);
        assertGauge(registry, "hestiastore_index_maintenance_active_threads",
                executorBase + 2D);
        assertFunctionCounter(registry,
                "hestiastore_index_maintenance_completed_tasks_total",
                executorBase + 3D);
        assertFunctionCounter(registry,
                "hestiastore_index_maintenance_rejected_tasks_total",
                executorBase + 4D);
        assertGauge(registry, "hestiastore_split_maintenance_queue_size",
                executorBase + 5D);
        assertGauge(registry, "hestiastore_split_maintenance_queue_capacity",
                executorBase + 6D);
        assertGauge(registry, "hestiastore_split_maintenance_active_threads",
                executorBase + 7D);
        assertFunctionCounter(registry,
                "hestiastore_split_maintenance_completed_tasks_total",
                executorBase + 8D);
        assertFunctionCounter(registry,
                "hestiastore_split_maintenance_rejected_tasks_total",
                executorBase + 9D);
        assertGauge(registry,
                "hestiastore_stable_segment_maintenance_queue_size",
                executorBase + 10D);
        assertGauge(registry,
                "hestiastore_stable_segment_maintenance_queue_capacity",
                executorBase + 11D);
        assertGauge(registry,
                "hestiastore_stable_segment_maintenance_active_threads",
                executorBase + 12D);
        assertFunctionCounter(registry,
                "hestiastore_stable_segment_maintenance_completed_tasks_total",
                executorBase + 13D);
        assertFunctionCounter(registry,
                "hestiastore_stable_segment_maintenance_caller_runs_total",
                executorBase + 14D);
        assertGauge(registry, "hestiastore_index_up", indexUp);
    }

    private static void assertFunctionCounter(
            final SimpleMeterRegistry registry, final String meterName,
            final double expected) {
        assertEquals(expected,
                registry.get(meterName).tag("index", "orders")
                        .functionCounter().count());
    }

    private static void assertGauge(final SimpleMeterRegistry registry,
            final String meterName, final double expected) {
        assertEquals(expected,
                registry.get(meterName).tag("index", "orders").gauge().value());
    }
}
