package org.hestiastore.index.segmentindex.core.runtime;

import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.control.IndexControlPlane;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.core.control.SegmentRuntimeLimitApplier;
import org.hestiastore.index.segmentindex.core.durability.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.core.maintenance.SegmentIndexMaintenanceAccess;
import org.hestiastore.index.segmentindex.core.operation.SegmentIndexOperationAccess;

/**
 * Holds runtime services that sit on top of storage and split state.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexRuntimeServices<K, V> {

    private final IndexWalCoordinator<K, V> walCoordinator;
    private final SegmentIndexOperationAccess<K, V> operationAccess;
    private final SegmentIndexMaintenanceAccess<K, V> maintenanceAccess;
    private final Supplier<SegmentIndexMetricsSnapshot> metricsSnapshotSupplier;
    private final IndexControlPlane controlPlane;
    private final SegmentRuntimeLimitApplier<K, V> runtimeLimitApplier;

    SegmentIndexRuntimeServices(
            final IndexWalCoordinator<K, V> walCoordinator,
            final SegmentIndexOperationAccess<K, V> operationAccess,
            final SegmentIndexMaintenanceAccess<K, V> maintenanceAccess,
            final SegmentRuntimeLimitApplier<K, V> runtimeLimitApplier,
            final Supplier<SegmentIndexMetricsSnapshot> metricsSnapshotSupplier,
            final IndexControlPlane controlPlane) {
        this.walCoordinator = Vldtn.requireNonNull(walCoordinator,
                "walCoordinator");
        this.operationAccess = Vldtn.requireNonNull(
                operationAccess, "operationAccess");
        this.maintenanceAccess = Vldtn.requireNonNull(maintenanceAccess,
                "maintenanceAccess");
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

    SegmentIndexMaintenanceAccess<K, V> maintenanceAccess() {
        return maintenanceAccess;
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
