package org.hestiastore.index.datatype;

import java.util.Comparator;

import org.hestiastore.index.Bytes;

public class TypeDescriptorInteger implements TypeDescriptor<Integer> {

    /**
     * Thombstone value, use can't use it.
     */
    public static final Integer TOMBSTONE_VALUE = Integer.MIN_VALUE + 1;

    /**
     * How many bytes is required to store Integer.
     */
    static final int REQUIRED_BYTES = 4;

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
        return this::getBytesBuffer;
    }

    @Override
    public ConvertorFromBytes<Integer> getConvertorFromBytes() {
        return bytes -> load(bytes, 0);
    }

    @Override
    public TypeReader<Integer> getTypeReader() {
        return fileReader -> {
            final Bytes buffer = Bytes.allocate(REQUIRED_BYTES);
            if (fileReader.read(buffer) == -1) {
                return null;
            }
            return load(buffer, 0);
        };
    }

    @Override
    public TypeWriter<Integer> getTypeWriter() {
        return (writer, object) -> {
            final Bytes encoded = getBytesBuffer(object);
            writer.write(encoded);
            return encoded.length();
        };
    }

    private Bytes getBytesBuffer(final Integer value) {
        int pos = 0;
        int v = value.intValue();
        final Bytes out = Bytes.allocate(REQUIRED_BYTES);
        final byte[] data = out.getData();
        data[pos++] = (byte) ((v >>> BYTE_SHIFT_24) & BYTE_MASK);
        data[pos++] = (byte) ((v >>> BYTE_SHIFT_16) & BYTE_MASK);
        data[pos++] = (byte) ((v >>> BYTE_SHIFT_8) & BYTE_MASK);
        data[pos] = (byte) ((v >>> BYTE_SHIFT_0) & BYTE_MASK);
        return out;
    }

    private Integer load(final Bytes data, final int from) {
        if (data.length() < from + REQUIRED_BYTES) {
            throw new IllegalArgumentException(
                    "Not enough bytes to read an Integer value");
        }
        final byte[] raw = data.getData();
        int pos = from;
        return raw[pos++] << BYTE_SHIFT_24
                | (raw[pos++] & BYTE_MASK) << BYTE_SHIFT_16
                | (raw[pos++] & BYTE_MASK) << BYTE_SHIFT_8
                | (raw[pos] & BYTE_MASK);
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
