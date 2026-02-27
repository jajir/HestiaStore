package org.hestiastore.index.chunkstore;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.bytes.ByteSequence;

/**
 * A writer for writing chunks to a chunk store.
 */
public interface ChunkStoreWriter extends CloseableResource {

    /**
     * Writes a chunk payload sequence to the chunk store.
     *
     * @param chunkPayload required chunk payload sequence to write.
     * @param version      required version of the chunk.
     * @return The position of the written chunk in the chunk store.
     */
    CellPosition writeSequence(ByteSequence chunkPayload, int version);

}
