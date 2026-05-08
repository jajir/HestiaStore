package org.hestiastore.index.segmentindex.runtimemonitoring;

import java.time.Instant;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;

/**
 * Read-only runtime snapshot view for one index instance.
 */
public final class IndexRuntimeMonitoringImpl
        implements IndexRuntimeMonitoring {

    private final EffectiveIndexConfiguration<?, ?> conf;
    private final Supplier<SegmentIndexState> stateSupplier;
    private final Supplier<SegmentIndexMetricsSnapshot> metricsSnapshotSupplier;

    public IndexRuntimeMonitoringImpl(
            final EffectiveIndexConfiguration<?, ?> conf,
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
