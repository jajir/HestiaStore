package org.hestiastore.monitoring.micrometer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.IndexRuntimeSnapshot;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.SegmentIndexBloomFilterMetrics;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.SegmentIndexChunkStoreCacheMetrics;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.SegmentIndexExecutorMetrics;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.SegmentIndexLatencyMetrics;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.SegmentIndexMaintenanceMetrics;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.SegmentIndexOperationMetrics;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.SegmentIndexRegistryCacheMetrics;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.SegmentIndexSegmentMetrics;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.SegmentIndexSplitMetrics;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.SegmentIndexWalMetrics;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.SegmentIndexWritePathMetrics;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

class HestiaStoreMicrometerBinderTest {

    @SuppressWarnings("unchecked")
    @Test
    void bindTo_exposesDynamicCountersAndStateGauge() {
        final SegmentIndex<Integer, String> index = mock(SegmentIndex.class);
        final IndexRuntimeMonitoring runtimeMonitoring = mock(
                IndexRuntimeMonitoring.class);
        final AtomicReference<IndexRuntimeSnapshot> snapshotRef = new AtomicReference<>(
                snapshot(1L, 2L, 3L, SegmentIndexState.READY, 7, 11, 29,
                        37L, 2, 50));
        when(index.runtimeMonitoring()).thenReturn(runtimeMonitoring);
        when(runtimeMonitoring.snapshot()).thenAnswer(inv -> snapshotRef.get());

        final SimpleMeterRegistry registry = new SimpleMeterRegistry();
        new HestiaStoreMicrometerBinder(
                new MicrometerSegmentIndexSource("orders", index))
                        .bindTo(registry);

        assertMetrics(registry, 1D, 2D, 3D, 7D, 11D, 29D, 37D, 2D, 1D, 50);

        snapshotRef.set(snapshot(5L, 8L, 13L, SegmentIndexState.CLOSED, 9, 13,
                15, 41L, 0, 70));

        assertMetrics(registry, 5D, 8D, 13D, 9D, 13D, 15D, 41D, 0D, 0D, 70);
    }

    private IndexRuntimeSnapshot snapshot(final long getCount,
            final long putCount, final long deleteCount,
            final SegmentIndexState state, final int segmentWriteCacheKeyLimit,
            final int segmentWriteCacheKeyLimitDuringMaintenance,
            final int indexBufferedWriteKeyLimit,
            final long splitScheduleCount, final int splitInFlightCount,
            final int executorBase) {
        final SegmentIndexExecutorMetrics indexExecutor =
                new SegmentIndexExecutorMetrics(executorBase + 2, executorBase,
                        executorBase + 1, executorBase + 3L,
                        executorBase + 4L, 0L);
        final SegmentIndexExecutorMetrics splitExecutor =
                new SegmentIndexExecutorMetrics(executorBase + 7,
                        executorBase + 5, executorBase + 6,
                        executorBase + 8L, executorBase + 9L, 0L);
        final SegmentIndexExecutorMetrics stableExecutor =
                new SegmentIndexExecutorMetrics(executorBase + 12,
                        executorBase + 10, executorBase + 11,
                        executorBase + 13L, 0L, executorBase + 14L);
        return new IndexRuntimeSnapshot(
                "orders",
                state,
                Instant.EPOCH,
                new SegmentIndexOperationMetrics(getCount, putCount,
                        deleteCount),
                new SegmentIndexRegistryCacheMetrics(0L, 0L, 0L, 0L, 0, 0),
                new SegmentIndexChunkStoreCacheMetrics(0, 0, 0L, 0L, 0L, 0L,
                        0L, 0L),
                new SegmentIndexSegmentMetrics(0, 0, 0, 0, 0, 0, 0, 0L, 0L,
                        0L, java.util.List.of()),
                new SegmentIndexWritePathMetrics(segmentWriteCacheKeyLimit,
                        segmentWriteCacheKeyLimitDuringMaintenance,
                        indexBufferedWriteKeyLimit, 0L),
                new SegmentIndexMaintenanceMetrics(0L, 0L, executorBase + 22L,
                        executorBase + 23L, executorBase + 24L,
                        executorBase + 25L, indexExecutor, stableExecutor),
                new SegmentIndexSplitMetrics(splitScheduleCount,
                        splitInFlightCount, 0, executorBase + 15L,
                        executorBase + 16L, splitExecutor),
                new SegmentIndexLatencyMetrics(0L, 0L, 0L, 0L, 0L, 0L),
                new SegmentIndexBloomFilterMetrics(0, 0, 0D, 0L, 0L, 0L, 0L),
                new SegmentIndexWalMetrics(false, 0L, 0L, 0L, 0L, 0L, 0L,
                        0L, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L));
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
