package org.hestiastore.monitoring.micrometer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

class HestiaStoreMicrometerBinderTest {

    @SuppressWarnings("unchecked")
    @Test
    void bindTo_exposesDynamicCountersAndStateGauge() {
        final SegmentIndex<Integer, String> index = mock(SegmentIndex.class);
        final AtomicReference<SegmentIndexMetricsSnapshot> snapshotRef = new AtomicReference<>(
                snapshot(1L, 2L, 3L, SegmentIndexState.READY, 7, 2, 11, 29, 3,
                        2, 1, 4, 17, 19L, 23L, 31L, 5, 43L, 37L, 2));
        when(index.metricsSnapshot()).thenAnswer(inv -> snapshotRef.get());
        when(index.getState()).thenAnswer(inv -> snapshotRef.get().getState());

        final SimpleMeterRegistry registry = new SimpleMeterRegistry();
        new HestiaStoreMicrometerBinder(
                new MicrometerSegmentIndexSource("orders", index))
                        .bindTo(registry);

        assertMetrics(registry, 1D, 2D, 3D, 7D, 2D, 11D, 29D, 3D, 2D, 1D, 4D,
                17D, 19D, 23D, 31D, 5D, 43D, 37D, 2D, 1D);

        snapshotRef.set(snapshot(5L, 8L, 13L, SegmentIndexState.CLOSED, 9, 3,
                15, 41, 4, 1, 0, 2, 5, 29L, 31L, 37L, 0, 0L, 41L, 0));

        assertMetrics(registry, 5D, 8D, 13D, 9D, 3D, 15D, 41D, 4D, 1D, 0D, 2D,
                5D, 29D, 31D, 37D, 0D, 0D, 41D, 0D, 0D);
    }

    private SegmentIndexMetricsSnapshot snapshot(final long getCount,
            final long putCount, final long deleteCount,
            final SegmentIndexState state,
            final int activePartitionLimit,
            final int immutableRunLimit,
            final int partitionBufferLimit, final int indexBufferLimit,
            final int partitionCount, final int activePartitionCount,
            final int drainingPartitionCount, final int immutableRunCount,
            final int partitionBufferedKeyCount,
            final long localThrottleCount,
            final long globalThrottleCount,
            final long drainScheduleCount, final int drainInFlightCount,
            final long drainLatencyP95Micros,
            final long splitScheduleCount, final int splitInFlightCount) {
        return new SegmentIndexMetricsSnapshot(
                getCount, putCount, deleteCount,
                0L, 0L, 0L, 0L,
                0, 0,
                0, activePartitionLimit, partitionBufferLimit,
                0, 0, 0, 0, 0, 0,
                0L, 0L, 0L, 0L,
                0L, 0L, splitScheduleCount,
                splitInFlightCount, 0, 0, 0, 0,
                0L, 0L, 0L,
                0L, 0L, 0L,
                0, 0, 0D,
                0L, 0L, 0L, 0L,
                false, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                0, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                immutableRunLimit, indexBufferLimit, partitionCount,
                activePartitionCount, drainingPartitionCount,
                immutableRunCount, partitionBufferedKeyCount,
                localThrottleCount, globalThrottleCount, drainScheduleCount,
                drainInFlightCount, drainLatencyP95Micros, List.of(),
                state);
    }

    private static void assertMetrics(final SimpleMeterRegistry registry,
            final double getCount, final double putCount,
            final double deleteCount, final double activePartitionLimit,
            final double immutableRunLimit,
            final double partitionBufferLimit, final double indexBufferLimit,
            final double partitionCount, final double activePartitionCount,
            final double drainingPartitionCount,
            final double immutableRunCount,
            final double partitionBufferedKeyCount,
            final double localThrottleCount,
            final double globalThrottleCount,
            final double drainScheduleCount, final double drainInFlightCount,
            final double drainLatencyP95Micros,
            final double splitScheduleCount, final double splitInFlightCount,
            final double indexUp) {
        assertFunctionCounter(registry, "hestiastore_ops_get_total", getCount);
        assertFunctionCounter(registry, "hestiastore_ops_put_total", putCount);
        assertFunctionCounter(registry, "hestiastore_ops_delete_total",
                deleteCount);
        assertGauge(registry, "hestiastore_partition_active_limit",
                activePartitionLimit);
        assertGauge(registry, "hestiastore_partition_immutable_run_limit",
                immutableRunLimit);
        assertGauge(registry, "hestiastore_partition_buffer_limit",
                partitionBufferLimit);
        assertGauge(registry, "hestiastore_index_buffer_limit",
                indexBufferLimit);
        assertGauge(registry, "hestiastore_partition_count", partitionCount);
        assertGauge(registry, "hestiastore_partition_active_count",
                activePartitionCount);
        assertGauge(registry, "hestiastore_partition_draining_count",
                drainingPartitionCount);
        assertGauge(registry, "hestiastore_partition_immutable_run_count",
                immutableRunCount);
        assertGauge(registry, "hestiastore_partition_buffered_key_count",
                partitionBufferedKeyCount);
        assertFunctionCounter(registry,
                "hestiastore_partition_throttle_local_total",
                localThrottleCount);
        assertFunctionCounter(registry,
                "hestiastore_partition_throttle_global_total",
                globalThrottleCount);
        assertFunctionCounter(registry,
                "hestiastore_partition_drain_schedule_total",
                drainScheduleCount);
        assertGauge(registry, "hestiastore_partition_drain_in_flight",
                drainInFlightCount);
        assertGauge(registry, "hestiastore_partition_drain_latency_p95_micros",
                drainLatencyP95Micros);
        assertFunctionCounter(registry, "hestiastore_split_schedule_total",
                splitScheduleCount);
        assertGauge(registry, "hestiastore_split_in_flight", splitInFlightCount);
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
