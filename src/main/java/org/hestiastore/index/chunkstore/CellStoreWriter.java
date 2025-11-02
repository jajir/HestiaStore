package org.hestiastore.index.chunkstore;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.CloseableResource;

/**
 * Allows to write bytes into underlying block store.
 */
public interface CellStoreWriter extends CloseableResource {

    CellPosition write(ByteSequence bytes);

}
