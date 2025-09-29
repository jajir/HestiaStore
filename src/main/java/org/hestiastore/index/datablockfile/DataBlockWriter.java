package org.hestiastore.index.datablockfile;

import org.hestiastore.index.CloseableResource;

/**
 * A writer for data blocks.
 */
public interface DataBlockWriter extends CloseableResource {

    void write(DataBlockPayload dataBlockPayload);

}
