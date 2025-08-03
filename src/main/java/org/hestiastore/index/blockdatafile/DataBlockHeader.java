package org.hestiastore.index.blockdatafile;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.ConvertorFromBytes;
import org.hestiastore.index.datatype.ConvertorToBytes;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorLong;

public class DataBlockHeader {

    private static final TypeDescriptor<Long> TYPE_DESCRIPTOR_LONG = new TypeDescriptorLong();
    private static final ConvertorFromBytes<Long> CONVERTOR_FROM_BYTES = TYPE_DESCRIPTOR_LONG
            .getConvertorFromBytes();
    private static final ConvertorToBytes<Long> CONVERTOR_TO_BYTES = TYPE_DESCRIPTOR_LONG
            .getConvertorToBytes();

    private long magicNumber;
    private long crc;

    public static DataBlockHeader of(final Bytes bytes) {
        Vldtn.requireNonNull(bytes, "bytes");
        return new DataBlockHeader(extractMagicNumber(bytes),
                extractCrc(bytes));
    }

    public static DataBlockHeader of(final Long magicNumber, final Long crc) {
        return new DataBlockHeader(magicNumber, crc);
    }

    private DataBlockHeader(final Long magicNumber, final Long crc) {
        this.magicNumber = Vldtn.requireNonNull(magicNumber, "magicNumber");
        this.crc = Vldtn.requireNonNull(crc, "crc");
    }

    public long getMagicNumber() {
        return magicNumber;
    }

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

    public Bytes toBytes() {
        final byte[] out = new byte[DataBlock.HEADER_SIZE];
        System.arraycopy(CONVERTOR_TO_BYTES.toBytes(magicNumber), 0, out, 0, 8);
        System.arraycopy(CONVERTOR_TO_BYTES.toBytes(crc), 0, out, 8, 8);
        return Bytes.of(out);
    }

}
