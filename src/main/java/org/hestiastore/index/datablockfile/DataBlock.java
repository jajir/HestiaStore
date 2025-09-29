package org.hestiastore.index.datablockfile;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;

/**
 * Represents a block of data in the block data file. All blocks have a fixed
 * size.
 */
public final class DataBlock {

    private final Bytes bytes;

    private final DataBlockPosition position;

    public static DataBlock of(final Bytes bytes,
            final DataBlockPosition position) {
        return new DataBlock(bytes, position);
    }

    DataBlock(final Bytes bytes, final DataBlockPosition position) {
        this.bytes = Vldtn.requireNonNull(bytes, "bytes");
        this.position = Vldtn.requireNonNull(position, "position");
        final DataBlockHeader header = getHeader();
        if (header.getMagicNumber() != DataBlockHeader.MAGIC_NUMBER) {
            throw new IllegalArgumentException(
                    "Invalid magic number in data block header");
        }
        if (header.getCrc() != calculateCrc()) {
            throw new IllegalArgumentException(
                    "CRC mismatch in data block header");
        }
    }

    /**
     * Get the payload of this data block.
     * 
     * @return the payload of this data block
     */
    public DataBlockPayload getPayload() {
        return DataBlockPayload.of(
                bytes.subBytes(DataBlockHeader.HEADER_SIZE, bytes.length()));
    }

    /**
     * Get the header of this data block.
     * 
     * @return the header of this data block
     */
    public DataBlockHeader getHeader() {
        return DataBlockHeader
                .of(bytes.subBytes(0, DataBlockHeader.HEADER_SIZE));
    }

    /**
     * Get the raw bytes of this data block.
     * 
     * @return the raw bytes of this data block
     */
    public Bytes getBytes() {
        return bytes;
    }

    /**
     * Get the position of this data block in the data block file.
     * 
     * @return the position of this data block in the data block file
     */
    public DataBlockPosition getPosition() {
        return position;
    }

    /**
     * Validate the integrity of this data block by checking its magic number
     * and CRC.
     * 
     * @throws IllegalArgumentException if the magic number or CRC is invalid
     */
    void validate() {
        final DataBlockHeader header = getHeader();
        if (header.getMagicNumber() != DataBlockHeader.MAGIC_NUMBER) {
            throw new IllegalArgumentException(
                    "Invalid magic number in data block header");
        }
        if (header.getCrc() != calculateCrc()) {
            throw new IllegalArgumentException(
                    "CRC mismatch in data block header");
        }
    }

    /**
     * Calculate the CRC of the payload of this data block.
     * 
     * @return the CRC of the payload of this data block
     */
    long calculateCrc() {
        return getPayload().calculateCrc();
    }

}
