package org.hestiastore.index.datablockfile;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;

/**
 * DataBlockHeader represents the header of a data block.It have fixed size of
 * 16 bytes, where the first 8 bytes are the magic number and the next 8 bytes
 * are the CRC of the data block. It's probably coincidence that it have size of
 * one 16 byte cell.
 */
public class DataBlockHeader {

    /**
     * Size of header in bytes. Header is fixed size of 16 bytes (8 bytes magic
     * number + 8 bytes CRC).
     */
    public static final int HEADER_SIZE = 16;

    /**
     * "nicholas" in ASCII
     */
    public static final long MAGIC_NUMBER = 0x6E6963686F6C6173L;

    private long magicNumber;
    private long crc;

    /**
     * Create DataBlockHeader from bytes. Bytes must be exactly 16 bytes long.
     *
     * @param bytes Bytes representing the DataBlockHeader.
     * @return DataBlockHeader
     */
    public static DataBlockHeader of(final Bytes bytes) {
        Vldtn.requireNonNull(bytes, "bytes");
        final byte[] data = bytes.getData();
        if (data.length < HEADER_SIZE) {
            throw new IllegalArgumentException(String.format(
                    "Data block header requires at least '%s' bytes, but got '%s'.",
                    HEADER_SIZE, data.length));
        }
        return new DataBlockHeader(readLong(data, 0), readLong(data, 8));
    }

    /**
     * Create DataBlockHeader from magic number and CRC.
     *
     * @param magicNumber required Magic number
     * @param crc         required CRC
     * @return DataBlockHeader instance
     */
    public static DataBlockHeader of(final Long magicNumber, final Long crc) {
        return new DataBlockHeader(magicNumber, crc);
    }

    private DataBlockHeader(final Long magicNumber, final Long crc) {
        this.magicNumber = Vldtn.requireNonNull(magicNumber, "magicNumber");
        this.crc = Vldtn.requireNonNull(crc, "crc");
    }

    /**
     * Get magic number.
     *
     * @return magic number
     */
    public long getMagicNumber() {
        return magicNumber;
    }

    /**
     * Get CRC.
     *
     * @return CRC
     */
    public long getCrc() {
        return crc;
    }

    /**
     * Convert DataBlockHeader to bytes.
     *
     * @return Bytes representing the DataBlockHeader.
     */
    public Bytes toBytes() {
        final byte[] out = new byte[HEADER_SIZE];
        writeLong(out, 0, magicNumber);
        writeLong(out, 8, crc);
        return Bytes.of(out);
    }

    private static long readLong(final byte[] data, final int offset) {
        return ((long) data[offset] & 0xFFL) << 56
                | ((long) data[offset + 1] & 0xFFL) << 48
                | ((long) data[offset + 2] & 0xFFL) << 40
                | ((long) data[offset + 3] & 0xFFL) << 32
                | ((long) data[offset + 4] & 0xFFL) << 24
                | ((long) data[offset + 5] & 0xFFL) << 16
                | ((long) data[offset + 6] & 0xFFL) << 8
                | ((long) data[offset + 7] & 0xFFL);
    }

    private static void writeLong(final byte[] data, final int offset,
            final long value) {
        data[offset] = (byte) (value >>> 56);
        data[offset + 1] = (byte) (value >>> 48);
        data[offset + 2] = (byte) (value >>> 40);
        data[offset + 3] = (byte) (value >>> 32);
        data[offset + 4] = (byte) (value >>> 24);
        data[offset + 5] = (byte) (value >>> 16);
        data[offset + 6] = (byte) (value >>> 8);
        data[offset + 7] = (byte) value;
    }

}
