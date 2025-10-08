package org.hestiastore.index.chunkstore;

/**
 * A chunk filter that does nothing. Filter just passes the chunk data through
 * unchanged.
 */
public class ChunkFilterDoNothing implements ChunkFilter {

    @Override
    public ChunkData apply(final ChunkData data) {
        return data;
    }

}
