package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.operations.SegmentIndexOperationAccess;
import org.hestiastore.index.segmentindex.core.storage.CoreStorageRuntime;
import org.hestiastore.index.segmentindex.core.storage.StorageService;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;

/**
 * Names the runtime-side collaborators needed to assemble the session-facing
 * index handle without passing the whole runtime aggregate.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentIndexSessionAssemblyInput<K, V> {

    private final SegmentIndexOperationAccess<K, V> operationAccess;
    private final SegmentTopologyRuntimeAccess<K, V> topologyRuntime;
    private final MaintenanceService maintenance;
    private final RuntimeTuning runtimeTuning;
    private final IndexRuntimeMonitoring runtimeMonitoring;
    private final CoreStorageRuntime<K, V> coreStorageRuntime;
    private final StorageService<K, V> storageService;

    /**
     * Creates session assembly input from concrete runtime collaborators.
     *
     * @param operationAccess point-operation and WAL replay access
     * @param topologyRuntime topology and iterator runtime access
     * @param maintenance maintenance service
     * @param runtimeTuning public runtime tuning view
     * @param runtimeMonitoring public runtime monitoring view
     * @param coreStorageRuntime core storage lifecycle owner
     * @param storageService storage service and WAL close owner
     */
    public SegmentIndexSessionAssemblyInput(
            final SegmentIndexOperationAccess<K, V> operationAccess,
            final SegmentTopologyRuntimeAccess<K, V> topologyRuntime,
            final MaintenanceService maintenance,
            final RuntimeTuning runtimeTuning,
            final IndexRuntimeMonitoring runtimeMonitoring,
            final CoreStorageRuntime<K, V> coreStorageRuntime,
            final StorageService<K, V> storageService) {
        this.operationAccess = Vldtn.requireNonNull(operationAccess,
                "operationAccess");
        this.topologyRuntime = Vldtn.requireNonNull(topologyRuntime,
                "topologyRuntime");
        this.maintenance = Vldtn.requireNonNull(maintenance, "maintenance");
        this.runtimeTuning = Vldtn.requireNonNull(runtimeTuning,
                "runtimeTuning");
        this.runtimeMonitoring = Vldtn.requireNonNull(runtimeMonitoring,
                "runtimeMonitoring");
        this.coreStorageRuntime = Vldtn.requireNonNull(coreStorageRuntime,
                "coreStorageRuntime");
        this.storageService = Vldtn.requireNonNull(storageService,
                "storageService");
    }

    SegmentIndexOperationAccess<K, V> operationAccess() {
        return operationAccess;
    }

    SegmentTopologyRuntimeAccess<K, V> topologyRuntime() {
        return topologyRuntime;
    }

    MaintenanceService maintenance() {
        return maintenance;
    }

    RuntimeTuning runtimeTuning() {
        return runtimeTuning;
    }

    IndexRuntimeMonitoring runtimeMonitoring() {
        return runtimeMonitoring;
    }

    CoreStorageRuntime<K, V> coreStorageRuntime() {
        return coreStorageRuntime;
    }

    StorageService<K, V> storageService() {
        return storageService;
    }
}
