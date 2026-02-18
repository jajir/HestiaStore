package org.hestiastore.index.chunkstore;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.datablockfile.Reader;

/**
 * A reader for sequential reading chunks from a chunk store.
 */
public interface ChunkStoreReader extends CloseableResource, Reader<Chunk> {

}
