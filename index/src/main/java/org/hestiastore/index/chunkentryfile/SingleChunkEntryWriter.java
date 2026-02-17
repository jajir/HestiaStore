package org.hestiastore.index.chunkentryfile;

import org.hestiastore.index.Entry;
import org.hestiastore.index.chunkstore.ChunkPayload;

/**
 * It's interface for in-memmory structure representing one chunk.
 */
public interface SingleChunkEntryWriter<K, V> {

    /**
     * Puts a entry into the chunk.
     * 
     * @param entry required entry to put
     */
    void put(Entry<K, V> entry);

    /**
     * Closes the chunk and returns the payload to be written to the chunk
     * store.
     * 
     * @return chunk payload
     */
    ChunkPayload close();

}
