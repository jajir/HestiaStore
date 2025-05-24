package com.hestiastore.index.datatype;

import java.util.Comparator;

public class TypeDescriptorInteger implements TypeDescriptor<Integer> {

    /**
     * Thombstone value, use can't use it.
     */
    public final static Integer TOMBSTONE_VALUE = Integer.MIN_VALUE + 1;

    /**
     * How many bytes is required to store Integer.
     */
    private static final int REQUIRED_BYTES = 4;

    /**
     * With byte AND allows to select required part of bytes.
     */
    private static final int BYTE_MASK = 0xFF;

    /**
     * Bite shift for 0 bits.
     */
    private static final int BYTE_SHIFT_0 = 0;

    /**
     * Bite shift for 8 bits.
     */
    private static final int BYTE_SHIFT_8 = 8;

    /**
     * Bite shift for 16 bits.
     */
    private static final int BYTE_SHIFT_16 = 16;

    /**
     * Bite shift for 24 bits.
     */
    private static final int BYTE_SHIFT_24 = 24;

    @Override
    public ConvertorToBytes<Integer> getConvertorToBytes() {
        return object -> getBytes(object);
    }

    @Override
    public ConvertorFromBytes<Integer> getConvertorFromBytes() {
        return bytes -> load(bytes, 0);
    }

    @Override
    public TypeReader<Integer> getTypeReader() {
        return fileReader -> {
            final byte[] bytes = new byte[4];
            if (fileReader.read(bytes) == -1) {
                return null;
            }
            return load(bytes, 0);
        };
    }

    @Override
    public TypeWriter<Integer> getTypeWriter() {
        return (writer, object) -> {
            writer.write(getBytes(object));
            return 4;
        };
    }

    private byte[] getBytes(final Integer value) {
        int pos = 0;
        int v = value.intValue();
        byte[] out = new byte[REQUIRED_BYTES];
        out[pos++] = (byte) ((v >>> BYTE_SHIFT_24) & BYTE_MASK);
        out[pos++] = (byte) ((v >>> BYTE_SHIFT_16) & BYTE_MASK);
        out[pos++] = (byte) ((v >>> BYTE_SHIFT_8) & BYTE_MASK);
        out[pos] = (byte) ((v >>> BYTE_SHIFT_0) & BYTE_MASK);
        return out;
    }

    private Integer load(final byte[] data, final int from) {
        int pos = from;
        return data[pos++] << BYTE_SHIFT_24
                | (data[pos++] & BYTE_MASK) << BYTE_SHIFT_16
                | (data[pos++] & BYTE_MASK) << BYTE_SHIFT_8
                | (data[pos] & BYTE_MASK);
    }

    @Override
    public Comparator<Integer> getComparator() {
        return (i1, i2) -> i1 - i2;
    }

    @Override
    public Integer getTombstone() {
        return TOMBSTONE_VALUE;
    }

}
