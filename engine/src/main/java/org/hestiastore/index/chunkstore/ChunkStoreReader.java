package org.hestiastore.index.chunkstore;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.datablockfile.Reader;

/**
 * A reader for sequential reading chunks from a chunk store.
 */
public interface ChunkStoreReader extends CloseableResource, Reader<Chunk> {

    /**
     * Reads payload of the next chunk as byte sequence.
     *
     * @return payload sequence or {@code null} when no more chunks are
     *         available
     */
    default ByteSequence readPayloadSequence() {
        final Chunk chunk = read();
        if (chunk == null) {
            return null;
        }
        return chunk.getPayloadSequence();
    }

}
