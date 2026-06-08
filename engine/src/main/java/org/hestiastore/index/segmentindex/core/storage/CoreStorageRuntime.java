package org.hestiastore.index.segmentindex.core.storage;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;

/**
 * Storage runtime collaborators assembled after physical storage is open.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class CoreStorageRuntime<K, V> {

    private final RuntimeTuningState runtimeTuningState;
    private final StorageService<K, V> storageService;

    /**
     * Creates storage runtime collaborators.
     *
     * @param runtimeTuningState runtime tuning state derived from configuration
     * @param storageService storage package entry point
     */
    public CoreStorageRuntime(final RuntimeTuningState runtimeTuningState,
            final StorageService<K, V> storageService) {
        this.runtimeTuningState = Vldtn.requireNonNull(runtimeTuningState,
                "runtimeTuningState");
        this.storageService = Vldtn.requireNonNull(storageService,
                "storageService");
    }

    /**
     * Returns the runtime tuning state used by runtime services.
     *
     * @return runtime tuning state
     */
    public RuntimeTuningState getRuntimeTuningState() {
        return runtimeTuningState;
    }

    /**
     * Returns the storage package entry point.
     *
     * @return storage service
     */
    public StorageService<K, V> getStorageService() {
        return storageService;
    }
}
