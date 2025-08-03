package org.hestiastore.index.blockdatafile;

import org.hestiastore.index.CloseableResource;

public interface DataBlockReader extends CloseableResource, Reader<DataBlock> {

}
