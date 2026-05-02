package org.hestiastore.index.segmentindex.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.junit.jupiter.api.Test;

class RuntimeMetricsCollectorImplTest {

    @Test
    void metricsSnapshotReturnsSupplierSnapshot() {
        final SegmentIndexMetricsSnapshot expected = emptySnapshot();
        final RuntimeMetricsCollector service = new RuntimeMetricsCollectorImpl(() -> expected);

        assertSame(expected, service.metricsSnapshot());
    }

    @Test
    void constructorRejectsNullSnapshotSupplier() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new RuntimeMetricsCollectorImpl(null));

        assertEquals("Property 'snapshotSupplier' must not be null.",
                ex.getMessage());
    }

    private static SegmentIndexMetricsSnapshot emptySnapshot() {
        return new SegmentIndexMetricsSnapshot(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0,
                0, 0, 0, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0, 0D, 0L, 0L, 0L,
                0L, List.of(), SegmentIndexState.READY);
    }
}
