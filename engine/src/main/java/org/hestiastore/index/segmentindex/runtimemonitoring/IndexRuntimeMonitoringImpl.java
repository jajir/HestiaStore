package org.hestiastore.index.segmentindex.runtimemonitoring;

import java.time.Instant;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.metrics.RuntimeMetricsCollector;

/**
 * Read-only runtime snapshot view for one index instance.
 */
public final class IndexRuntimeMonitoringImpl
        implements IndexRuntimeMonitoring {

    private final EffectiveIndexConfiguration<?, ?> conf;
    private final Supplier<SegmentIndexState> stateSupplier;
    private final RuntimeMetricsCollector metricsCollector;

    public IndexRuntimeMonitoringImpl(
            final EffectiveIndexConfiguration<?, ?> conf,
            final Supplier<SegmentIndexState> stateSupplier,
            final RuntimeMetricsCollector metricsCollector) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.stateSupplier = Vldtn.requireNonNull(stateSupplier,
                "stateSupplier");
        this.metricsCollector = Vldtn.requireNonNull(metricsCollector,
                "metricsCollector");
    }

    @Override
    public IndexRuntimeSnapshot snapshot() {
        return new IndexRuntimeSnapshot(conf.identity().name(),
                stateSupplier.get(), metricsCollector.metricsSnapshot(),
                Instant.now());
    }
}
