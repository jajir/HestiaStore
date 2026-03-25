package org.hestiastore.index.segmentindex.config;

import org.hestiastore.index.chunkstore.ChunkData;
import org.hestiastore.index.chunkstore.ChunkFilter;

public final class LegacyCustomChunkFilter implements ChunkFilter {

    @Override
    public ChunkData apply(final ChunkData input) {
        return input;
    }
}
