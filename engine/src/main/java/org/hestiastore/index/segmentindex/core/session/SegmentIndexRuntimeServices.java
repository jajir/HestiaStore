package org.hestiastore.index.segmentindex.core.session;

import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.configuration.tuning.SegmentRuntimeLimitApplier;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.operations.SegmentIndexOperationAccess;
import org.hestiastore.index.segmentindex.core.storage.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.metrics.RuntimeMetricsCollector;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;

/**
 * Holds runtime services that sit on top of storage and split state.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentIndexRuntimeServices<K, V> {

    private final IndexWalCoordinator<K, V> walCoordinator;
    private final SegmentIndexOperationAccess<K, V> operationAccess;
    private final MaintenanceService maintenance;
    private final Supplier<SegmentIndexMetricsSnapshot> metricsSnapshotSupplier;
    private final IndexRuntimeMonitoring runtimeMonitoring;
    private final RuntimeTuning runtimeConfiguration;
    private final SegmentRuntimeLimitApplier<K, V> runtimeLimitApplier;

    public SegmentIndexRuntimeServices(
            final IndexWalCoordinator<K, V> walCoordinator,
            final SegmentIndexOperationAccess<K, V> operationAccess,
            final MaintenanceService maintenance,
            final SegmentRuntimeLimitApplier<K, V> runtimeLimitApplier,
            final RuntimeMetricsCollector metricsCollector,
            final IndexRuntimeMonitoring runtimeMonitoring,
            final RuntimeTuning runtimeConfiguration) {
        this.walCoordinator = Vldtn.requireNonNull(walCoordinator,
                "walCoordinator");
        this.operationAccess = Vldtn.requireNonNull(
                operationAccess, "operationAccess");
        this.maintenance = Vldtn.requireNonNull(maintenance, "maintenance");
        this.runtimeLimitApplier = Vldtn.requireNonNull(runtimeLimitApplier,
                "runtimeLimitApplier");
        this.metricsSnapshotSupplier = Vldtn.requireNonNull(metricsCollector,
                "metricsCollector")::metricsSnapshot;
        this.runtimeMonitoring = Vldtn.requireNonNull(runtimeMonitoring,
                "runtimeMonitoring");
        this.runtimeConfiguration = Vldtn.requireNonNull(runtimeConfiguration,
                "runtimeConfiguration");
    }

    IndexWalCoordinator<K, V> walCoordinator() {
        return walCoordinator;
    }

    SegmentIndexOperationAccess<K, V> operationAccess() {
        return operationAccess;
    }

    MaintenanceService maintenance() {
        return maintenance;
    }

    Supplier<SegmentIndexMetricsSnapshot> metricsSnapshotSupplier() {
        return metricsSnapshotSupplier;
    }

    IndexRuntimeMonitoring runtimeMonitoring() {
        return runtimeMonitoring;
    }

    RuntimeTuning runtimeTuning() {
        return runtimeConfiguration;
    }

    SegmentRuntimeLimitApplier<K, V> runtimeLimitApplier() {
        return runtimeLimitApplier;
    }
}
