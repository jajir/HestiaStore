package org.hestiastore.index.datablockfile;

import org.hestiastore.index.Vldtn;

/**
 * Class represents data block size value and payloadSize value.
 */
public class DataBlockSize {

    private final int size;

    public static DataBlockSize ofDataBlockSize(final int size) {
        return new DataBlockSize(size);
    }

    private DataBlockSize(final int size) {
        this.size = Vldtn.requiredIoBufferSize(size);
    }

    /**
     * Get data block size in bytes including header and payload.
     * 
     * @return datablock size in bytes
     */
    public int getDataBlockSize() {
        return size;
    }

    public int getPayloadSize() {
        return size - DataBlockHeader.HEADER_SIZE;
    }

}
