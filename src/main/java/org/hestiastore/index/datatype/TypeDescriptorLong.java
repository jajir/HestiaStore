package org.hestiastore.index.datatype;

import java.util.Comparator;

import org.hestiastore.index.Bytes;

public class TypeDescriptorLong implements TypeDescriptor<Long> {

    /**
     * Thombstone value, use can't use it.
     */
    public static final Long TOMBSTONE_VALUE = Long.MIN_VALUE + 1;

    /**
     * How many bytes is required to store Integer.
     */
    static final int REQUIRED_BYTES = 8;

    /**
     * With byte AND allows to select required part of bytes.
     */
    private static final long BYTE_MASK = 0xFFL;

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

    /**
     * Bite shift for 32 bits.
     */
    private static final int BYTE_SHIFT_32 = 32;

    /**
     * Bite shift for 40 bits.
     */
    private static final int BYTE_SHIFT_40 = 40;

    /**
     * Bite shift for 48 bits.
     */
    private static final int BYTE_SHIFT_48 = 48;

    /**
     * Bite shift for 56 bits.
     */
    private static final int BYTE_SHIFT_56 = 56;

    @Override
    public ConvertorToBytes<Long> getConvertorToBytes() {
        return this::getBytesBuffer;
    }

    @Override
    public ConvertorFromBytes<Long> getConvertorFromBytes() {
        return bytes -> load(bytes, 0);
    }

    @Override
    public TypeReader<Long> getTypeReader() {
        return fileReader -> {
            final Bytes buffer = Bytes.allocate(REQUIRED_BYTES);
            if (fileReader.read(buffer) == -1) {
                return null;
            }
            return load(buffer, 0);
        };
    }

    @Override
    public TypeWriter<Long> getTypeWriter() {
        return (writer, object) -> {
            final Bytes encoded = getBytesBuffer(object);
            writer.write(encoded);
            return encoded.length();
        };
    }

    private Bytes getBytesBuffer(final Long value) {
        int pos = 0;
        long v = value.longValue();
        final Bytes out = Bytes.allocate(REQUIRED_BYTES);
        final byte[] data = out.getData();
        data[pos++] = (byte) ((v >>> BYTE_SHIFT_56) & BYTE_MASK);
        data[pos++] = (byte) ((v >>> BYTE_SHIFT_48) & BYTE_MASK);
        data[pos++] = (byte) ((v >>> BYTE_SHIFT_40) & BYTE_MASK);
        data[pos++] = (byte) ((v >>> BYTE_SHIFT_32) & BYTE_MASK);
        data[pos++] = (byte) ((v >>> BYTE_SHIFT_24) & BYTE_MASK);
        data[pos++] = (byte) ((v >>> BYTE_SHIFT_16) & BYTE_MASK);
        data[pos++] = (byte) ((v >>> BYTE_SHIFT_8) & BYTE_MASK);
        data[pos] = (byte) ((v >>> BYTE_SHIFT_0) & BYTE_MASK);
        return out;
    }

    private Long load(final Bytes data, final int from) {
        if (data.length() < from + REQUIRED_BYTES) {
            throw new IllegalArgumentException(
                    "Not enough bytes to read a Long value");
        }
        final byte[] raw = data.getData();
        int pos = from;
        return ((long) (raw[pos++] & BYTE_MASK) << BYTE_SHIFT_56)
                | ((long) (raw[pos++] & BYTE_MASK) << BYTE_SHIFT_48)
                | ((long) (raw[pos++] & BYTE_MASK) << BYTE_SHIFT_40)
                | ((long) (raw[pos++] & BYTE_MASK) << BYTE_SHIFT_32)
                | ((long) (raw[pos++] & BYTE_MASK) << BYTE_SHIFT_24)
                | ((long) (raw[pos++] & BYTE_MASK) << BYTE_SHIFT_16)
                | ((long) (raw[pos++] & BYTE_MASK) << BYTE_SHIFT_8)
                | ((long) (raw[pos] & BYTE_MASK) << BYTE_SHIFT_0);
    }

    @Override
    public Comparator<Long> getComparator() {
        return (i1, i2) -> i1.compareTo(i2);
    }

    @Override
    public Long getTombstone() {
        return TOMBSTONE_VALUE;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        return obj != null && obj.getClass() == getClass();
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

}
