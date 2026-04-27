package org.hestiastore.index.segmentindex.core.metrics;

import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;

/**
 * Default metric service implementation.
 */
final class MetricServiceImpl implements MetricService {

    private final Supplier<SegmentIndexMetricsSnapshot> snapshotSupplier;

    MetricServiceImpl(
            final Supplier<SegmentIndexMetricsSnapshot> snapshotSupplier) {
        this.snapshotSupplier = Vldtn.requireNonNull(snapshotSupplier,
                "snapshotSupplier");
    }

    @Override
    public SegmentIndexMetricsSnapshot metricsSnapshot() {
        return snapshotSupplier.get();
    }
}
