package org.hestiastore.index.datablockfile;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;

/**
 * Represents a block of data in the block data file.
 */
public class DataBlock {

    public static final int HEADER_SIZE = 16;

    // "nicholas" in ASCII
    public static final long MAGIC_NUMBER = 0x6E6963686F6C6173L;

    private final Bytes bytes;

    private final DataBlockPosition position;

    public static DataBlock of(final Bytes bytes,
            final DataBlockPosition position) {
        return new DataBlock(bytes, position);
    }

    DataBlock(final Bytes bytes, final DataBlockPosition position) {
        this.bytes = Vldtn.requireNonNull(bytes, "bytes");
        this.position = Vldtn.requireNonNull(position, "position");
        if (getHeader().getMagicNumber() != MAGIC_NUMBER) {
            throw new IllegalArgumentException(
                    "Invalid magic number in data block header");
        }
    }

    public DataBlockPayload getPayload() {
        return new DataBlockPayload(
                bytes.subBytes(HEADER_SIZE, bytes.length()));
    }

    public DataBlockHeader getHeader() {
        return DataBlockHeader.of(bytes.subBytes(0, HEADER_SIZE));
    }

    public Bytes getBytes() {
        return bytes;
    }

    public DataBlockPosition getPosition() {
        return position;
    }

    void validate() {
        final DataBlockHeader header = getHeader();
        if (header.getMagicNumber() != MAGIC_NUMBER) {
            throw new IllegalArgumentException(
                    "Invalid magic number in data block header");
        }
        if (header.getCrc() != calculateCrc()) {
            throw new IllegalArgumentException(
                    "CRC mismatch in data block header");
        }
    }

    public long calculateCrc() {
        return getPayload().calculateCrc();
    }

}
