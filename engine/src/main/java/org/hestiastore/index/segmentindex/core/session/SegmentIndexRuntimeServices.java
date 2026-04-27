package org.hestiastore.index.segmentindex.core.session;

import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.control.IndexControlPlane;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.core.control.SegmentRuntimeLimitApplier;
import org.hestiastore.index.segmentindex.core.storage.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.operations.SegmentIndexOperationAccess;

/**
 * Holds runtime services that sit on top of storage and split state.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexRuntimeServices<K, V> {

    private final IndexWalCoordinator<K, V> walCoordinator;
    private final SegmentIndexOperationAccess<K, V> operationAccess;
    private final MaintenanceService maintenance;
    private final Supplier<SegmentIndexMetricsSnapshot> metricsSnapshotSupplier;
    private final IndexControlPlane controlPlane;
    private final SegmentRuntimeLimitApplier<K, V> runtimeLimitApplier;

    SegmentIndexRuntimeServices(
            final IndexWalCoordinator<K, V> walCoordinator,
            final SegmentIndexOperationAccess<K, V> operationAccess,
            final MaintenanceService maintenance,
            final SegmentRuntimeLimitApplier<K, V> runtimeLimitApplier,
            final Supplier<SegmentIndexMetricsSnapshot> metricsSnapshotSupplier,
            final IndexControlPlane controlPlane) {
        this.walCoordinator = Vldtn.requireNonNull(walCoordinator,
                "walCoordinator");
        this.operationAccess = Vldtn.requireNonNull(
                operationAccess, "operationAccess");
        this.maintenance = Vldtn.requireNonNull(maintenance, "maintenance");
        this.runtimeLimitApplier = Vldtn.requireNonNull(runtimeLimitApplier,
                "runtimeLimitApplier");
        this.metricsSnapshotSupplier = Vldtn.requireNonNull(
                metricsSnapshotSupplier, "metricsSnapshotSupplier");
        this.controlPlane = Vldtn.requireNonNull(controlPlane, "controlPlane");
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

    IndexControlPlane controlPlane() {
        return controlPlane;
    }

    SegmentRuntimeLimitApplier<K, V> runtimeLimitApplier() {
        return runtimeLimitApplier;
    }
}
