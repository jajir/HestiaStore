package org.hestiastore.index.chunkstore;

import org.hestiastore.index.CloseableResource;

public interface ChunkStoreWriter extends CloseableResource {

    CellPosition write(ChunkPayload chunkPayload, int version);

}
