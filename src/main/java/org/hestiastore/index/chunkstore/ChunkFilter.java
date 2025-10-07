package org.hestiastore.index.chunkstore;

/**
 * Interface for processing chunk data.
 */
public interface ChunkFilter {

    ChunkData apply(ChunkData input);

}
