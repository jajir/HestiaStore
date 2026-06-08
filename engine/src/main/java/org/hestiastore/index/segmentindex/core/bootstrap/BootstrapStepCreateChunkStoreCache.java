package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.chunkstorecache.LruChunkStoreCache;

/**
 * Creates the index-scoped parsed chunk page cache.
 */
final class BootstrapStepCreateChunkStoreCache<K, V>
        extends SegmentIndexBootstrapStep<K, V> {

    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        state.setChunkStoreCache(new LruChunkStoreCache<>(
                state.getConfiguration().chunkStoreCache().pageLimit()));
    }
}
