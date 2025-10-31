package org.hestiastore.index.datablockfile;

import org.hestiastore.index.ByteSequence;
import org.hestiastore.index.CloseableResource;

/**
 * Read an exact number of bytes from a sequence of blocks (using the cursor).
 */
public interface DataBlockByteReader extends CloseableResource {

    /**
     * Read exactly the specified number of bytes, spanning multiple blocks if
     * necessary.
     * 
     * @param length the number of bytes to read
     * @return the bytes read
     */
    ByteSequence readExactly(int length);

}
