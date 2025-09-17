package org.hestiastore.index.datablockfile;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.CloseableResource;

/**
 * Read an exact number of bytes from a sequence of blocks (using the cursor).
 */
public interface DataBlockByteReader extends CloseableResource {

    Bytes readExactly(int length); // may span multiple blocks

}
