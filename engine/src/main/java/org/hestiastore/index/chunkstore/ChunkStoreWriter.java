package org.hestiastore.index.chunkstore;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.IndexException;
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

    /**
     * Appends a fully encoded chunk without applying this writer's filters. The
     * caller must have applied the complete encoding chain, including any
     * compression and checksum, and retain immutable payload bytes until this
     * call returns. Only preparation may run concurrently; appends and
     * lifecycle operations on one writer remain single-threaded.
     *
     * @param chunk encoded chunk with its final flags, checksum and version
     * @return position of the appended chunk
     */
    default CellPosition writePreparedChunk(final ChunkData chunk) {
        throw new IndexException(
                "This writer does not support prepared chunks.");
    }

}
