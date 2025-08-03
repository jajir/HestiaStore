package org.hestiastore.index.blockdatafile;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;

/**
 * Represents a block of data in the block data file.
 */
public class DataBlock {

    static final int HEADER_SIZE = 16;

    // "nicholas" in ASCII
    static final long MAGIC_NUMBER = 0x6E6963686F6C6173L;

    private final Bytes bytes;

    private final BlockPosition position;

    DataBlock(final Bytes bytes, final BlockPosition position) {
        this.bytes = Vldtn.requireNonNull(bytes, "bytes");
        this.position = Vldtn.requireNonNull(position, "position");
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

    public BlockPosition getPosition() {
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
