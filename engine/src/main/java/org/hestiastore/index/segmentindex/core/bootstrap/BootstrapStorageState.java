package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstorecache.ChunkStoreCache;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.storage.CoreStorageRuntime;
import org.hestiastore.index.segmentindex.core.storage.StorageService;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Holds storage products created during bootstrap.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class BootstrapStorageState<K, V> {

    private KeyToSegmentMap<K> keyToSegmentMap;
    private ChunkStoreCache<K, V> chunkStoreCache;
    private SegmentRegistry<K, V> segmentRegistry;
    private CoreStorageRuntime<K, V> coreStorageRuntime;

    void setKeyToSegmentMap(final KeyToSegmentMap<K> keyToSegmentMap) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
    }

    boolean hasKeyToSegmentMap() {
        return keyToSegmentMap != null;
    }

    KeyToSegmentMap<K> getKeyToSegmentMap() {
        return requireInitialized(keyToSegmentMap, "keyToSegmentMap");
    }

    void setChunkStoreCache(
            final ChunkStoreCache<K, V> chunkStoreCache) {
        this.chunkStoreCache = Vldtn.requireNonNull(chunkStoreCache,
                "chunkStoreCache");
    }

    ChunkStoreCache<K, V> getChunkStoreCache() {
        return requireInitialized(chunkStoreCache, "chunkStoreCache");
    }

    void setSegmentRegistry(
            final SegmentRegistry<K, V> segmentRegistry) {
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
    }

    SegmentRegistry<K, V> getSegmentRegistry() {
        return requireInitialized(segmentRegistry, "segmentRegistry");
    }

    void setCoreStorageRuntime(
            final CoreStorageRuntime<K, V> coreStorageRuntime) {
        this.coreStorageRuntime = Vldtn.requireNonNull(coreStorageRuntime,
                "coreStorageRuntime");
    }

    boolean hasCoreStorage() {
        return coreStorageRuntime != null;
    }

    RuntimeTuningState getRuntimeTuningState() {
        return getCoreStorageRuntime().getRuntimeTuningState();
    }

    StorageService<K, V> getStorageService() {
        return getCoreStorageRuntime().getStorageService();
    }

    CoreStorageRuntime<K, V> getCoreStorageRuntime() {
        return requireInitialized(coreStorageRuntime, "coreStorageRuntime");
    }

    private static <T> T requireInitialized(final T value,
            final String fieldName) {
        if (value == null) {
            throw new IllegalStateException(
                    fieldName + " was not initialized.");
        }
        return value;
    }
}
