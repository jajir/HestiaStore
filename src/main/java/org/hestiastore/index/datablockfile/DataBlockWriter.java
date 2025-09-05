package org.hestiastore.index.datablockfile;

import org.hestiastore.index.CloseableResource;

public interface DataBlockWriter extends CloseableResource {

    void write(DataBlockPayload dataBlockPayload);

}
