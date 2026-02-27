package org.hestiastore.index.datablockfile;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequenceCrc32;

/**
 * Represents a block of data in the block data file. All blocks have a fixed
 * size.
 */
public final class DataBlock {

    private final ByteSequence bytes;

    private final DataBlockPosition position;

    /**
     * Creates a data block from byte sequence.
     *
     * @param bytes    full data block bytes (header + payload)
     * @param position data block position
     * @return data block instance
     */
    public static DataBlock ofSequence(final ByteSequence bytes,
            final DataBlockPosition position) {
        return new DataBlock(bytes, position);
    }

    DataBlock(final ByteSequence bytes, final DataBlockPosition position) {
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
     * Get the payload bytes of this data block as sequence.
     *
     * @return payload bytes sequence
     */
    public ByteSequence getPayloadSequence() {
        return bytes.slice(DataBlockHeader.HEADER_SIZE, bytes.length());
    }

    /**
     * Get the header of this data block.
     * 
     * @return the header of this data block
     */
    public DataBlockHeader getHeader() {
        return DataBlockHeader.ofSequence(bytes);
    }

    /**
     * Get raw bytes as byte sequence.
     *
     * @return full data block bytes sequence
     */
    public ByteSequence getBytesSequence() {
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
        final ByteSequenceCrc32 crc = new ByteSequenceCrc32();
        crc.update(bytes.slice(DataBlockHeader.HEADER_SIZE, bytes.length()));
        return crc.getValue();
    }

}
