package org.hestiastore.index.chunkstore;

import org.hestiastore.index.CloseableResource;

/**
 * A writer for writing chunks to a chunk store.
 */
public interface ChunkStoreWriter extends CloseableResource {

    /**
     * Writes a chunk payload to the chunk store.
     *
     * @param chunkPayload required chunk payload to write.
     * @param version      required version of the chunk.
     * @return The position of the written chunk in the chunk store.
     */
    CellPosition write(ChunkPayload chunkPayload, int version);

}
