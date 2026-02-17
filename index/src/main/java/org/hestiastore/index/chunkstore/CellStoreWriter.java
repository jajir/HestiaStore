package org.hestiastore.index.chunkstore;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.CloseableResource;

/**
 * Allows to write bytes into underlying block store.
 */
public interface CellStoreWriter extends CloseableResource {

    CellPosition write(Bytes bytes);

}
