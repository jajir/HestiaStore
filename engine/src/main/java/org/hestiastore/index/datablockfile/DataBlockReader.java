package org.hestiastore.index.datablockfile;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.bytes.ByteSequence;

/**
 * Reads DataBlocks from a DataBlockFile. It read sequentionally from the start
 * to the end of the file.
 */
public interface DataBlockReader extends CloseableResource, Reader<DataBlock> {

    /**
     * Reads payload bytes of the next data block as sequence.
     *
     * @return payload sequence or {@code null} when end of stream is reached
     */
    default ByteSequence readPayloadSequence() {
        final DataBlock dataBlock = read();
        if (dataBlock == null) {
            return null;
        }
        return dataBlock.getPayloadSequence();
    }

}
