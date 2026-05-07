package org.hestiastore.index.segmentindex.configuration.persistence;

import org.hestiastore.index.chunkstore.ChunkData;
import org.hestiastore.index.chunkstore.ChunkFilter;

public final class LegacyCustomChunkFilter implements ChunkFilter {

    @Override
    public ChunkData apply(final ChunkData input) {
        return input;
    }
}
