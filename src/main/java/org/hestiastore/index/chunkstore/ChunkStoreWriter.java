package org.hestiastore.index.chunkstore;

import org.hestiastore.index.CloseableResource;

public interface ChunkStoreWriter extends CloseableResource {

    void write(ChunkPayload chunkPayload);

}
