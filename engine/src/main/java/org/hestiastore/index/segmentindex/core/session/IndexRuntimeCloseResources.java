package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.storage.CoreStorageRuntime;
import org.hestiastore.index.segmentindex.core.storage.StorageService;

/**
 * Provides close-only access to runtime lifecycle collaborators used during
 * normal index shutdown.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class IndexRuntimeCloseResources<K, V> {

    private final SegmentTopologyRuntimeAccess<K, V> topologyRuntime;
    private final MaintenanceService maintenance;
    private final CoreStorageRuntime<K, V> coreStorageRuntime;
    private final StorageService<K, V> storageService;

    /**
     * Creates close-only runtime resources.
     *
     * @param topologyRuntime topology and split runtime close owner
     * @param maintenance maintenance service close owner
     * @param coreStorageRuntime core storage close owner
     * @param storageService storage service that owns WAL coordination
     */
    IndexRuntimeCloseResources(
            final SegmentTopologyRuntimeAccess<K, V> topologyRuntime,
            final MaintenanceService maintenance,
            final CoreStorageRuntime<K, V> coreStorageRuntime,
            final StorageService<K, V> storageService) {
        this.topologyRuntime = Vldtn.requireNonNull(topologyRuntime,
                "topologyRuntime");
        this.maintenance = Vldtn.requireNonNull(maintenance, "maintenance");
        this.coreStorageRuntime = Vldtn.requireNonNull(coreStorageRuntime,
                "coreStorageRuntime");
        this.storageService = Vldtn.requireNonNull(storageService,
                "storageService");
    }

    /**
     * Closes split runtime resources.
     */
    void closeSplitRuntime() {
        topologyRuntime.closeSplitRuntime();
    }

    /**
     * Seals asynchronous maintenance and waits for accepted work.
     */
    void sealAsyncMaintenanceAndWait() {
        maintenance.sealAsyncMaintenanceAndWait();
    }

    /**
     * Flushes durable maintenance work and waits for completion.
     */
    void flushAndWait() {
        maintenance.flushAndWait();
    }

    /**
     * Closes core storage resources.
     */
    void closeCoreStorage() {
        coreStorageRuntime.closeCoreStorage();
    }

    /**
     * Closes WAL coordination resources.
     */
    void closeWal() {
        storageService.closeWal();
    }
}
