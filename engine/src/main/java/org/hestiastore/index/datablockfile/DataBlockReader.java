package org.hestiastore.index.datablockfile;

import org.hestiastore.index.CloseableResource;

/**
 * Reads DataBlocks from a DataBlockFile. It read sequentionally from the start
 * to the end of the file.
 */
public interface DataBlockReader extends CloseableResource, Reader<DataBlock> {

}
