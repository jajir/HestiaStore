package org.hestiastore.index.blockdatafile;

import org.apache.commons.codec.digest.PureJavaCrc32;
import org.hestiastore.index.Bytes;

/**
 * Represents a block of data in the block data file.
 */
public class DataBlock {

    private static final int HEADER_SIZE = 16;

    // "hicholas" in ASCII
    private static final long MAGIC_NUMBER = 0x6E6963686F6C6173L;

    private final Bytes bytes;

    public DataBlock(final Bytes bytes) {
        this.bytes = bytes;
    }

    public DataBlockPayload getPayload() {
        return new DataBlockPayload(
                bytes.subBytes(HEADER_SIZE, bytes.length()));
    }

    public DataBlockHeader getHeader() {
        return new DataBlockHeader(bytes.subBytes(0, HEADER_SIZE));
    }

    public Bytes getBytes() {
        return bytes;
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
        final PureJavaCrc32 crc = new PureJavaCrc32();
        crc.update(getPayload().getBytes().getData());
        return crc.getValue();
    }

}
