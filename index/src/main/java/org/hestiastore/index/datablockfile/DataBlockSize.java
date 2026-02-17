package org.hestiastore.index.datablockfile;

import org.hestiastore.index.Vldtn;

/**
 * Class represents data block size value and payloadSize value.
 */
public class DataBlockSize {

    private final int size;

    /**
     * Creates a new data block size.
     * 
     * @param size the size of each data block including header and payload
     * @return new instance of DataBlockSize
     */
    public static DataBlockSize ofDataBlockSize(final int size) {
        return new DataBlockSize(size);
    }

    private DataBlockSize(final int size) {
        this.size = Vldtn.requireIoBufferSize(size);
    }

    /**
     * Get data block size in bytes including header and payload.
     * 
     * @return datablock size in bytes
     */
    public int getDataBlockSize() {
        return size;
    }

    /**
     * Get payload size in bytes.
     * 
     * @return payload size in bytes
     */
    public int getPayloadSize() {
        return size - DataBlockHeader.HEADER_SIZE;
    }

    @Override
    public String toString() {
        return "DataBlockSize [size=" + size + "]";
    }

    @Override
    public int hashCode() {
        return size;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!(obj instanceof DataBlockSize))
            return false;
        DataBlockSize other = (DataBlockSize) obj;
        return size == other.size;
    }

}
