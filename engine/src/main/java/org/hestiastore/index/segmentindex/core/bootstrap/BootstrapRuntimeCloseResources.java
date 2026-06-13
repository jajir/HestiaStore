package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.session.SegmentTopologyRuntimeAccess;
import org.hestiastore.index.segmentindex.core.storage.CoreStorageRuntime;
import org.hestiastore.index.segmentindex.core.storage.StorageService;

/**
 * Owns runtime resource cleanup after bootstrap has moved close ownership away
 * from the individual opening steps but before the session handle is returned.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class BootstrapRuntimeCloseResources<K, V> {

    private final SegmentTopologyRuntimeAccess<K, V> topologyRuntime;
    private final CoreStorageRuntime<K, V> coreStorageRuntime;
    private final StorageService<K, V> storageService;

    /**
     * Creates failed-bootstrap close resources.
     *
     * @param topologyRuntime topology runtime close owner
     * @param coreStorageRuntime core storage close owner
     * @param storageService storage service that owns WAL coordination
     */
    BootstrapRuntimeCloseResources(
            final SegmentTopologyRuntimeAccess<K, V> topologyRuntime,
            final CoreStorageRuntime<K, V> coreStorageRuntime,
            final StorageService<K, V> storageService) {
        this.topologyRuntime = Vldtn.requireNonNull(topologyRuntime,
                "topologyRuntime");
        this.coreStorageRuntime = Vldtn.requireNonNull(coreStorageRuntime,
                "coreStorageRuntime");
        this.storageService = Vldtn.requireNonNull(storageService,
                "storageService");
    }

    /**
     * Closes runtime resources after failed bootstrap initialization.
     */
    void closeAfterFailedInitialization() {
        RuntimeException failure = null;
        failure = closeSplitRuntime(failure);
        failure = closeCoreStorage(failure);
        failure = closeWal(failure);
        if (failure != null) {
            throw failure;
        }
    }

    private RuntimeException closeSplitRuntime(
            final RuntimeException failure) {
        try {
            topologyRuntime.closeSplitRuntime();
            return failure;
        } catch (final RuntimeException cleanupFailure) {
            return appendCleanupFailure(failure, cleanupFailure);
        }
    }

    private RuntimeException closeCoreStorage(
            final RuntimeException failure) {
        try {
            coreStorageRuntime.closeCoreStorage();
            return failure;
        } catch (final RuntimeException cleanupFailure) {
            return appendCleanupFailure(failure, cleanupFailure);
        }
    }

    private RuntimeException closeWal(final RuntimeException failure) {
        try {
            storageService.closeWal();
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
