package org.hestiastore.index.segmentindex;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterSpec;

/**
 * Immutable persisted chunk filter pipeline settings view.
 */
public final class IndexFilterConfiguration {

    private final List<ChunkFilterSpec> encodingChunkFilters;
    private final List<ChunkFilterSpec> decodingChunkFilters;

    public IndexFilterConfiguration(final List<ChunkFilterSpec> encodingChunkFilters,
            final List<ChunkFilterSpec> decodingChunkFilters) {
        this.encodingChunkFilters = List.copyOf(encodingChunkFilters);
        this.decodingChunkFilters = List.copyOf(decodingChunkFilters);
    }

    public List<ChunkFilterSpec> encodingChunkFilterSpecs() {
        return encodingChunkFilters;
    }

    public List<ChunkFilterSpec> decodingChunkFilterSpecs() {
        return decodingChunkFilters;
    }
}
