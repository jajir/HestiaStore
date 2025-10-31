package org.hestiastore.index.datatype;

import java.util.Comparator;

import org.hestiastore.index.ByteSequence;
import org.hestiastore.index.MutableBytes;

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
            final MutableBytes buffer = MutableBytes.allocate(REQUIRED_BYTES);
            if (fileReader.read(buffer) == -1) {
                return null;
            }
            return load(buffer, 0);
        };
    }

    @Override
    public TypeWriter<Integer> getTypeWriter() {
        return (writer, object) -> {
            final ByteSequence encoded = getBytesBuffer(object);
            writer.write(encoded);
            return encoded.length();
        };
    }

    private ByteSequence getBytesBuffer(final Integer value) {
        int pos = 0;
        int v = value.intValue();
        final MutableBytes out = MutableBytes.allocate(REQUIRED_BYTES);
        out.setByte(pos++, (byte) ((v >>> BYTE_SHIFT_24) & BYTE_MASK));
        out.setByte(pos++, (byte) ((v >>> BYTE_SHIFT_16) & BYTE_MASK));
        out.setByte(pos++, (byte) ((v >>> BYTE_SHIFT_8) & BYTE_MASK));
        out.setByte(pos, (byte) ((v >>> BYTE_SHIFT_0) & BYTE_MASK));
        return out.toBytes();
    }

    private Integer load(final ByteSequence data, final int from) {
        if (data.length() < from + REQUIRED_BYTES) {
            throw new IllegalArgumentException(
                    "Not enough bytes to read an Integer value");
        }
        int pos = from;
        return data.getByte(pos++) << BYTE_SHIFT_24
                | (data.getByte(pos++) & BYTE_MASK) << BYTE_SHIFT_16
                | (data.getByte(pos++) & BYTE_MASK) << BYTE_SHIFT_8
                | (data.getByte(pos) & BYTE_MASK);
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
