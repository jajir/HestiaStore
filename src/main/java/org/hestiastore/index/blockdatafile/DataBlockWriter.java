package org.hestiastore.index.blockdatafile;

import org.hestiastore.index.CloseableResource;

public interface DataBlockWriter extends CloseableResource {

    void write(DataBlockPayload dataBlockPayload);

}
