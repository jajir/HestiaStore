package org.hestiastore.index.directory;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.CloseableResource;

public interface FileWriter extends CloseableResource {

    void write(byte b);

    /**
     * Write all bytes stored inside the provided Bytes instance.
     *
     * @param bytes required Bytes wrapper
     */
    void write(ByteSequence bytes);

}
