package org.hestiastore.index.chunkstore;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.datablockfile.CellPosition;

public interface ChunkStoreWriter extends CloseableResource {

    CellPosition write(ChunkPayload chunkPayload, int version);

}
