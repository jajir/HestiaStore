package com.coroptis.index.datatype;

import java.util.Comparator;

public class TypeDescriptorByte implements TypeDescriptor<Byte> {

    /**
     * Thombstone value, use can't use it.
     */
    private final static Byte TOMBSTONE_VALUE = Byte.MIN_VALUE;

    @Override
    public ConvertorToBytes<Byte> getConvertorToBytes() {
        return b -> {
            final byte[] out = new byte[1];
            out[0] = b;
            return out;
        };
    }

    @Override
    public ConvertorFromBytes<Byte> getConvertorFromBytes() {
        return bytes -> bytes[0];
    }

    @Override
    public TypeReader<Byte> getTypeReader() {
        return inputStream -> (byte) inputStream.read();
    }

    @Override
    public TypeWriter<Byte> getTypeWriter() {
        return (fileWriter, b) -> {
            fileWriter.write(b);
            return 1;
        };
    }

    @Override
    public Comparator<Byte> getComparator() {
        return (i1, i2) -> i2 - i1;
    }

    @Override
    public Byte getTombstone() {
        return TOMBSTONE_VALUE;
    }

}
