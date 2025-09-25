package org.hestiastore.index.datablockfile;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;

/**
 * Represents a block of data in the block data file.
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

    public DataBlockPayload getPayload() {
        return new DataBlockPayload(
                bytes.subBytes(DataBlockHeader.HEADER_SIZE, bytes.length()));
    }

    public DataBlockHeader getHeader() {
        return DataBlockHeader
                .of(bytes.subBytes(0, DataBlockHeader.HEADER_SIZE));
    }

    public Bytes getBytes() {
        return bytes;
    }

    public DataBlockPosition getPosition() {
        return position;
    }

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

    public long calculateCrc() {
        return getPayload().calculateCrc();
    }

}
