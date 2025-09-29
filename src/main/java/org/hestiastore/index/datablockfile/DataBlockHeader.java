package org.hestiastore.index.datablockfile;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.ConvertorFromBytes;
import org.hestiastore.index.datatype.ConvertorToBytes;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorLong;

/**
 * DataBlockHeader represents the header of a data block.It have fixed size of
 * 16 bytes, where the first 8 bytes are the magic number and the next 8 bytes
 * are the CRC of the data block. It's probably coincidence that it have size of
 * one 16 byte cell.
 */
public class DataBlockHeader {

    private static final TypeDescriptor<Long> TYPE_DESCRIPTOR_LONG = new TypeDescriptorLong();
    private static final ConvertorFromBytes<Long> CONVERTOR_FROM_BYTES = TYPE_DESCRIPTOR_LONG
            .getConvertorFromBytes();
    private static final ConvertorToBytes<Long> CONVERTOR_TO_BYTES = TYPE_DESCRIPTOR_LONG
            .getConvertorToBytes();

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
        return new DataBlockHeader(extractMagicNumber(bytes),
                extractCrc(bytes));
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

    private static long extractMagicNumber(final Bytes bytes) {
        final byte[] buff = bytes.subBytes(0, 8).getData();
        return CONVERTOR_FROM_BYTES.fromBytes(buff);
    }

    private static long extractCrc(final Bytes bytes) {
        final byte[] buff = bytes.subBytes(8, 16).getData();
        return CONVERTOR_FROM_BYTES.fromBytes(buff);
    }

    /**
     * Convert DataBlockHeader to bytes.
     *
     * @return Bytes representing the DataBlockHeader.
     */
    public Bytes toBytes() {
        final byte[] out = new byte[HEADER_SIZE];
        System.arraycopy(CONVERTOR_TO_BYTES.toBytes(magicNumber), 0, out, 0, 8);
        System.arraycopy(CONVERTOR_TO_BYTES.toBytes(crc), 0, out, 8, 8);
        return Bytes.of(out);
    }

}
