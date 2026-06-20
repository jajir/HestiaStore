package org.hestiastore.index.segmentindex.core.storage;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Storage runtime collaborators and lifecycle ownership assembled after
 * physical storage is open.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class OpenedStorageRuntime<K, V> {

    private final RuntimeTuningState runtimeTuningState;
    private final StorageCoordinator<K, V> storageService;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final SegmentRouteMap<K> keyToSegmentMap;

    /**
     * Creates storage runtime collaborators.
     *
     * @param runtimeTuningState runtime tuning state derived from configuration
     * @param storageService storage package entry point
     * @param segmentRegistry segment registry owned by the opened storage
     * @param keyToSegmentMap key-to-segment map owned by the opened storage
     */
    public OpenedStorageRuntime(final RuntimeTuningState runtimeTuningState,
            final StorageCoordinator<K, V> storageService,
            final SegmentRegistry<K, V> segmentRegistry,
            final SegmentRouteMap<K> keyToSegmentMap) {
        this.runtimeTuningState = Vldtn.requireNonNull(runtimeTuningState,
                "runtimeTuningState");
        this.storageService = Vldtn.requireNonNull(storageService,
                "storageService");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
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
    public StorageCoordinator<K, V> getStorageService() {
        return storageService;
    }

    /**
     * Closes the core storage resources in reverse ownership order.
     */
    public void closeCoreStorage() {
        RuntimeException failure = null;
        failure = closeSegmentRegistry(failure);
        failure = closeKeyToSegmentMap(failure);
        if (failure != null) {
            throw failure;
        }
    }

    private RuntimeException closeSegmentRegistry(
            final RuntimeException failure) {
        try {
            segmentRegistry.close();
            return failure;
        } catch (final RuntimeException cleanupFailure) {
            return appendCleanupFailure(failure, cleanupFailure);
        }
    }

    private RuntimeException closeKeyToSegmentMap(
            final RuntimeException failure) {
        if (keyToSegmentMap.wasClosed()) {
            return failure;
        }
        try {
            keyToSegmentMap.close();
            return failure;
        } catch (final RuntimeException cleanupFailure) {
            return appendCleanupFailure(failure, cleanupFailure);
        }
    }

    private RuntimeException appendCleanupFailure(
            final RuntimeException failure,
            final RuntimeException cleanupFailure) {
        if (failure == null) {
            return cleanupFailure;
        }
        failure.addSuppressed(cleanupFailure);
        return failure;
    }
}
