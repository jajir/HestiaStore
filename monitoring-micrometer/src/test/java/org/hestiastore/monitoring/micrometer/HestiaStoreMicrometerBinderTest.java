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
                        2, 1, 4, 17, 19L, 23L, 31L, 5));
        when(index.metricsSnapshot()).thenAnswer(inv -> snapshotRef.get());
        when(index.getState()).thenAnswer(inv -> snapshotRef.get().getState());

        final SimpleMeterRegistry registry = new SimpleMeterRegistry();
        new HestiaStoreMicrometerBinder(
                new MicrometerSegmentIndexSource("orders", index))
                        .bindTo(registry);

        assertEquals(1D,
                registry.get("hestiastore_ops_get_total").tag("index", "orders")
                        .functionCounter().count());
        assertEquals(2D,
                registry.get("hestiastore_ops_put_total").tag("index", "orders")
                        .functionCounter().count());
        assertEquals(3D, registry.get("hestiastore_ops_delete_total")
                .tag("index", "orders").functionCounter().count());
        assertEquals(7D, registry.get("hestiastore_partition_active_limit")
                .tag("index", "orders").gauge().value());
        assertEquals(2D, registry
                .get("hestiastore_partition_immutable_run_limit")
                .tag("index", "orders").gauge().value());
        assertEquals(11D, registry.get("hestiastore_partition_buffer_limit")
                .tag("index", "orders").gauge().value());
        assertEquals(29D, registry.get("hestiastore_index_buffer_limit")
                .tag("index", "orders").gauge().value());
        assertEquals(3D, registry.get("hestiastore_partition_count")
                .tag("index", "orders").gauge().value());
        assertEquals(2D, registry.get("hestiastore_partition_active_count")
                .tag("index", "orders").gauge().value());
        assertEquals(1D, registry.get("hestiastore_partition_draining_count")
                .tag("index", "orders").gauge().value());
        assertEquals(4D, registry
                .get("hestiastore_partition_immutable_run_count")
                .tag("index", "orders").gauge().value());
        assertEquals(17D, registry.get("hestiastore_partition_buffered_key_count")
                .tag("index", "orders").gauge().value());
        assertEquals(19D, registry
                .get("hestiastore_partition_throttle_local_total")
                .tag("index", "orders").functionCounter().count());
        assertEquals(23D, registry
                .get("hestiastore_partition_throttle_global_total")
                .tag("index", "orders").functionCounter().count());
        assertEquals(31D, registry
                .get("hestiastore_partition_drain_schedule_total")
                .tag("index", "orders").functionCounter().count());
        assertEquals(5D, registry.get("hestiastore_partition_drain_in_flight")
                .tag("index", "orders").gauge().value());
        assertEquals(1D, registry.get("hestiastore_index_up")
                .tag("index", "orders").gauge().value());

        snapshotRef.set(snapshot(5L, 8L, 13L, SegmentIndexState.CLOSED, 9, 3,
                15, 41, 4, 1, 0, 2, 5, 29L, 31L, 37L, 0));

        assertEquals(5D,
                registry.get("hestiastore_ops_get_total").tag("index", "orders")
                        .functionCounter().count());
        assertEquals(8D,
                registry.get("hestiastore_ops_put_total").tag("index", "orders")
                        .functionCounter().count());
        assertEquals(13D, registry.get("hestiastore_ops_delete_total")
                .tag("index", "orders").functionCounter().count());
        assertEquals(9D, registry.get("hestiastore_partition_active_limit")
                .tag("index", "orders").gauge().value());
        assertEquals(4D, registry.get("hestiastore_partition_count")
                .tag("index", "orders").gauge().value());
        assertEquals(31D, registry
                .get("hestiastore_partition_throttle_global_total")
                .tag("index", "orders").functionCounter().count());
        assertEquals(0D, registry.get("hestiastore_partition_drain_in_flight")
                .tag("index", "orders").gauge().value());
        assertEquals(0D, registry.get("hestiastore_index_up")
                .tag("index", "orders").gauge().value());
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
            final long drainScheduleCount, final int drainInFlightCount) {
        return new SegmentIndexMetricsSnapshot(
                getCount, putCount, deleteCount,
                0L, 0L, 0L, 0L,
                0, 0,
                0, activePartitionLimit, partitionBufferLimit,
                0, 0, 0, 0, 0, 0,
                0L, 0L, 0L, 0L,
                0L, 0L, 0L,
                0, 0, 0, 0, 0,
                0L, 0L, 0L,
                0L, 0L, 0L,
                0, 0, 0D,
                0L, 0L, 0L, 0L,
                immutableRunLimit, indexBufferLimit, partitionCount,
                activePartitionCount, drainingPartitionCount,
                immutableRunCount, partitionBufferedKeyCount,
                localThrottleCount, globalThrottleCount, drainScheduleCount,
                drainInFlightCount, List.of(),
                state);
    }
}
