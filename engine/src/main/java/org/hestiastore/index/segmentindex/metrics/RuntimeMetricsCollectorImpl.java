package org.hestiastore.index.segmentindex.metrics;

import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;

/**
 * Default runtime metrics collector implementation.
 */
final class RuntimeMetricsCollectorImpl implements RuntimeMetricsCollector {

    private final Supplier<SegmentIndexMetricsSnapshot> snapshotSupplier;

    RuntimeMetricsCollectorImpl(
            final Supplier<SegmentIndexMetricsSnapshot> snapshotSupplier) {
        this.snapshotSupplier = Vldtn.requireNonNull(snapshotSupplier,
                "snapshotSupplier");
    }

    @Override
    public SegmentIndexMetricsSnapshot metricsSnapshot() {
        return snapshotSupplier.get();
    }
}
