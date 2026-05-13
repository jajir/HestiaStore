package org.hestiastore.index.segmentindex.core.storage;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstorecache.ChunkStoreCache;
import org.hestiastore.index.chunkstorecache.LruChunkStoreCache;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Core storage collaborators opened before split and maintenance services are
 * created.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentIndexCoreStorage<K, V> {

    private final RuntimeTuningState runtimeTuningState;
    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final ChunkStoreCache<K, V> chunkStoreCache;
    private final IndexRetryPolicy retryPolicy;

    public SegmentIndexCoreStorage(final RuntimeTuningState runtimeTuningState,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final ChunkStoreCache<K, V> chunkStoreCache,
            final IndexRetryPolicy retryPolicy) {
        this.runtimeTuningState = Vldtn.requireNonNull(runtimeTuningState,
                "runtimeTuningState");
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.chunkStoreCache = Vldtn.requireNonNull(chunkStoreCache,
                "chunkStoreCache");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
    }

    public SegmentIndexCoreStorage(final RuntimeTuningState runtimeTuningState,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final IndexRetryPolicy retryPolicy) {
        this(runtimeTuningState, keyToSegmentMap, segmentRegistry,
                new LruChunkStoreCache<>(0), retryPolicy);
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
}
