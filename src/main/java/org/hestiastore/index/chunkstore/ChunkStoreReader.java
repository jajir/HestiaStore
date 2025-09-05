package org.hestiastore.index.chunkstore;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.datablockfile.Reader;

public interface ChunkStoreReader extends CloseableResource, Reader<Chunk> {

}
