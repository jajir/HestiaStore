package org.hestiastore.index.chunkstore;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.bytes.ByteSequence;

/**
 * Allows to write bytes into underlying block store.
 */
public interface CellStoreWriter extends CloseableResource {

    /**
     * Writes a sequence of bytes and returns start position where bytes were
     * written.
     *
     * @param bytes sequence to write, length must be cell-aligned
     * @return position of the first written byte
     */
    CellPosition writeSequence(ByteSequence bytes);

}
