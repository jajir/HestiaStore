package org.hestiastore.index.datablockfile;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.bytes.ByteSequence;

/**
 * A writer for data blocks.
 */
public interface DataBlockWriter extends CloseableResource {

    /**
     * Writes one data block payload.
     *
     * @param dataBlockPayload payload bytes sequence
     */
    void writeSequence(ByteSequence dataBlockPayload);

}
