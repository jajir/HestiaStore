package org.hestiastore.index.segmentindex.core.control;

import java.time.Instant;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.control.IndexRuntimeView;
import org.hestiastore.index.control.model.IndexRuntimeSnapshot;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;

/**
 * Read-only runtime snapshot view for one index instance.
 */
final class IndexRuntimeSnapshotView implements IndexRuntimeView {

    private final IndexConfiguration<?, ?> conf;
    private final Supplier<SegmentIndexState> stateSupplier;
    private final Supplier<SegmentIndexMetricsSnapshot> metricsSnapshotSupplier;

    IndexRuntimeSnapshotView(final IndexConfiguration<?, ?> conf,
            final Supplier<SegmentIndexState> stateSupplier,
            final Supplier<SegmentIndexMetricsSnapshot> metricsSnapshotSupplier) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.stateSupplier = Vldtn.requireNonNull(stateSupplier,
                "stateSupplier");
        this.metricsSnapshotSupplier = Vldtn.requireNonNull(
                metricsSnapshotSupplier, "metricsSnapshotSupplier");
    }

    @Override
    public IndexRuntimeSnapshot snapshot() {
        return new IndexRuntimeSnapshot(conf.identity().name(),
                stateSupplier.get(), metricsSnapshotSupplier.get(),
                Instant.now());
    }
}
