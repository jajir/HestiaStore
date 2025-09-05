package org.hestiastore.index.chunkpairfile;

import org.hestiastore.index.Pair;
import org.hestiastore.index.chunkstore.ChunkPayload;

/**
 * It's interface for in-memmory structure representing one chunk.
 */
public interface SingleChunkPairWriter<K, V> {

    void put(Pair<K, V> pair);

    ChunkPayload close();

}
