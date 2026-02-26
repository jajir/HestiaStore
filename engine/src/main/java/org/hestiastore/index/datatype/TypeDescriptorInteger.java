package org.hestiastore.index.datatype;

import java.util.Comparator;

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

    private static final ConvertorToBytes<Integer> CONVERTOR_TO_BYTES = new ConvertorToBytes<Integer>() {
        @Override
        public byte[] toBytes(final Integer object) {
            return getBytes(object);
        }

        @Override
        public int bytesLength(final Integer object) {
            return REQUIRED_BYTES;
        }

        @Override
        public int toBytes(final Integer object, final byte[] destination) {
            writeBytes(object, destination);
            return REQUIRED_BYTES;
        }
    };

    @Override
    public ConvertorToBytes<Integer> getConvertorToBytes() {
        return CONVERTOR_TO_BYTES;
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

    private static byte[] getBytes(final Integer value) {
        final byte[] out = new byte[REQUIRED_BYTES];
        writeBytes(value, out);
        return out;
    }

    private static void writeBytes(final Integer value,
            final byte[] destination) {
        if (destination.length < REQUIRED_BYTES) {
            throw new IllegalArgumentException(String.format(
                    "Destination buffer too small. Required '%s' but was '%s'",
                    REQUIRED_BYTES, destination.length));
        }
        int pos = 0;
        int v = value.intValue();
        destination[pos++] = (byte) ((v >>> BYTE_SHIFT_24) & BYTE_MASK);
        destination[pos++] = (byte) ((v >>> BYTE_SHIFT_16) & BYTE_MASK);
        destination[pos++] = (byte) ((v >>> BYTE_SHIFT_8) & BYTE_MASK);
        destination[pos] = (byte) ((v >>> BYTE_SHIFT_0) & BYTE_MASK);
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
