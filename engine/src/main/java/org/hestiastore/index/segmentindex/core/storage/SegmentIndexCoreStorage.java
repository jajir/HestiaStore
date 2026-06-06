package org.hestiastore.index.segmentindex.core.storage;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstorecache.ChunkStoreCache;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

//FIXME: This is service locator and in wrong package. Remove it.
/**
 * Core storage collaborators opened before split and maintenance services are
 * created.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentIndexCoreStorage<K, V>
        extends AbstractCloseableResource {

    private final RuntimeTuningState runtimeTuningState;
    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final ChunkStoreCache<K, V> chunkStoreCache;
    private final IndexRetryPolicy retryPolicy;
    private final StorageService<K, V> storageService;

    public SegmentIndexCoreStorage(final RuntimeTuningState runtimeTuningState,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final ChunkStoreCache<K, V> chunkStoreCache,
            final IndexRetryPolicy retryPolicy,
            final StorageService<K, V> storageService) {
        this.runtimeTuningState = Vldtn.requireNonNull(runtimeTuningState,
                "runtimeTuningState");
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.chunkStoreCache = Vldtn.requireNonNull(chunkStoreCache,
                "chunkStoreCache");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
        this.storageService = Vldtn.requireNonNull(storageService,
                "storageService");
    }

    public RuntimeTuningState runtimeTuningState() {
        return runtimeTuningState;
    }

    public KeyToSegmentMap<K> keyToSegmentMap() {
        return keyToSegmentMap;
    }

    public SegmentRegistry<K, V> segmentRegistry() {
        return segmentRegistry;
    }

    public ChunkStoreCache<K, V> chunkStoreCache() {
        return chunkStoreCache;
    }

    public IndexRetryPolicy retryPolicy() {
        return retryPolicy;
    }

    public StorageService<K, V> storageService() {
        return storageService;
    }

    @Override
    protected void doClose() {
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
