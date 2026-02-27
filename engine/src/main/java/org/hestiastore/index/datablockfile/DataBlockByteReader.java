package org.hestiastore.index.datablockfile;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.bytes.ByteSequence;

/**
 * Read an exact number of bytes from a sequence of blocks (using the cursor).
 */
public interface DataBlockByteReader extends CloseableResource {

    /**
     * Read exactly the specified number of bytes as sequence, spanning multiple
     * blocks if necessary.
     *
     * @param length the number of bytes to read
     * @return the bytes read as sequence, or {@code null} when no bytes are
     *         available
     */
    ByteSequence readExactlySequence(int length);

}
