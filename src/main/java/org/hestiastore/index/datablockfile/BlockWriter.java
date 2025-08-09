package org.hestiastore.index.datablockfile;

import org.hestiastore.index.Bytes;

public interface BlockWriter extends AutoCloseable {

    /**
     * Writes a DataBlock to the block file.
     *
     * @param blockPosition the position where the block should be written
     * @param dataBytes     the data bytes to write
     */
    DataBlockPosition write(Bytes blockData);

}
