package org.hestiastore.index.chunkpairfile;

import org.hestiastore.index.Pair;
import org.hestiastore.index.chunkstore.ChunkPayload;

/**
 * It's interface for in-memmory structure representing one chunk.
 */
public interface SingleChunkPairWriter<K, V> {

    /**
     * Puts a pair into the chunk.
     * 
     * @param pair required pair to put
     */
    void put(Pair<K, V> pair);

    /**
     * Closes the chunk and returns the payload to be written to the chunk
     * store.
     * 
     * @return chunk payload
     */
    ChunkPayload close();

}
