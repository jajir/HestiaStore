package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.storage.StorageService;
import org.hestiastore.index.segmentindex.maintenance.IndexConsistencyRepairService;

/**
 * Coordinates storage consistency repair with the topology rescan required
 * after repaired route state.
 */
final class StorageTopologyConsistencyRepairService
        implements IndexConsistencyRepairService {

    private final StorageService<?, ?> storageService;
    private final SegmentTopologyRuntimeAccess<?, ?> topologyRuntime;

    StorageTopologyConsistencyRepairService(
            final StorageService<?, ?> storageService,
            final SegmentTopologyRuntimeAccess<?, ?> topologyRuntime) {
        this.storageService = Vldtn.requireNonNull(storageService,
                "storageService");
        this.topologyRuntime = Vldtn.requireNonNull(topologyRuntime,
                "topologyRuntime");
    }

    @Override
    public void checkAndRepairConsistency() {
        storageService.checkAndRepairConsistency();
        topologyRuntime.requestFullSplitScan();
    }
}
