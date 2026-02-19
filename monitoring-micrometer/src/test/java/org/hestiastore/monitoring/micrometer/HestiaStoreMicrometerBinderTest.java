package org.hestiastore.monitoring.micrometer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
                new SegmentIndexMetricsSnapshot(1L, 2L, 3L,
                        SegmentIndexState.READY));
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
        assertEquals(1D, registry.get("hestiastore_index_up")
                .tag("index", "orders").gauge().value());

        snapshotRef.set(new SegmentIndexMetricsSnapshot(5L, 8L, 13L,
                SegmentIndexState.CLOSED));

        assertEquals(5D,
                registry.get("hestiastore_ops_get_total").tag("index", "orders")
                        .functionCounter().count());
        assertEquals(8D,
                registry.get("hestiastore_ops_put_total").tag("index", "orders")
                        .functionCounter().count());
        assertEquals(13D, registry.get("hestiastore_ops_delete_total")
                .tag("index", "orders").functionCounter().count());
        assertEquals(0D, registry.get("hestiastore_index_up")
                .tag("index", "orders").gauge().value());
    }
}
