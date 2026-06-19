package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationCoordinator;
import org.hestiastore.index.segmentindex.core.storage.CoreStorageRuntime;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;

/**
 * Test-only view of runtime collaborators extracted from an opened session
 * handle.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexRuntimeView<K, V> {

    private final CoreStorageRuntime<K, V> coreStorageRuntime;
    private final SegmentTopologyRuntimeAccess<K, V> topologyRuntime;
    private final IndexOperationCoordinator<K, V> operationAccess;
    private final MaintenanceService<K, V> maintenance;
    private final IndexRuntimeMonitoring runtimeMonitoring;
    private final RuntimeTuning runtimeTuning;

    /**
     * Creates a test runtime view.
     *
     * @param coreStorageRuntime core storage runtime
     * @param topologyRuntime topology runtime access
     * @param operationAccess operation access
     * @param maintenance maintenance service
     * @param runtimeMonitoring runtime monitoring view
     * @param runtimeTuning runtime tuning view
     */
    SegmentIndexRuntimeView(final CoreStorageRuntime<K, V> coreStorageRuntime,
            final SegmentTopologyRuntimeAccess<K, V> topologyRuntime,
            final IndexOperationCoordinator<K, V> operationAccess,
            final MaintenanceService<K, V> maintenance,
            final IndexRuntimeMonitoring runtimeMonitoring,
            final RuntimeTuning runtimeTuning) {
        this.coreStorageRuntime = Vldtn.requireNonNull(coreStorageRuntime,
                "coreStorageRuntime");
        this.topologyRuntime = Vldtn.requireNonNull(topologyRuntime,
                "topologyRuntime");
        this.operationAccess = Vldtn.requireNonNull(operationAccess,
                "operationAccess");
        this.maintenance = Vldtn.requireNonNull(maintenance, "maintenance");
        this.runtimeMonitoring = Vldtn.requireNonNull(runtimeMonitoring,
                "runtimeMonitoring");
        this.runtimeTuning = Vldtn.requireNonNull(runtimeTuning,
                "runtimeTuning");
    }

    CoreStorageRuntime<K, V> coreStorageRuntime() {
        return coreStorageRuntime;
    }

    SegmentTopologyRuntimeAccess<K, V> topologyRuntime() {
        return topologyRuntime;
    }

    IndexOperationCoordinator<K, V> operationAccess() {
        return operationAccess;
    }

    MaintenanceService<K, V> maintenance() {
        return maintenance;
    }

    IndexRuntimeMonitoring runtimeMonitoring() {
        return runtimeMonitoring;
    }

    RuntimeTuning runtimeTuning() {
        return runtimeTuning;
    }
}
