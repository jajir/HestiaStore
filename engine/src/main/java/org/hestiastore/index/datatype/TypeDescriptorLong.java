package org.hestiastore.index.datatype;

import java.util.Comparator;

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

    private static final TypeEncoder<Long> CONVERTOR_TO_BYTES = new TypeEncoder<Long>() {
        @Override
        public int bytesLength(final Long object) {
            return REQUIRED_BYTES;
        }

        @Override
        public int toBytes(final Long object, final byte[] destination) {
            writeBytes(object, destination);
            return REQUIRED_BYTES;
        }
    };

    @Override
    public TypeEncoder<Long> getTypeEncoder() {
        return CONVERTOR_TO_BYTES;
    }

    @Override
    public TypeDecoder<Long> getTypeDecoder() {
        return bytes -> load(bytes, 0);
    }

    @Override
    public TypeReader<Long> getTypeReader() {
        return fileReader -> {
            final byte[] bytes = new byte[8];
            if (!TypeIo.readFullyOrNull(fileReader, bytes)) {
                return null;
            }
            return load(bytes, 0);
        };
    }

    @Override
    public TypeWriter<Long> getTypeWriter() {
        return (writer, object) -> {
            writer.write(getBytes(object));
            return 8;
        };
    }

    private static byte[] getBytes(final Long value) {
        final byte[] out = new byte[REQUIRED_BYTES];
        writeBytes(value, out);
        return out;
    }

    private static void writeBytes(final Long value, final byte[] destination) {
        if (destination.length < REQUIRED_BYTES) {
            throw new IllegalArgumentException(String.format(
                    "Destination buffer too small. Required '%s' but was '%s'",
                    REQUIRED_BYTES, destination.length));
        }
        int pos = 0;
        long v = value.longValue();
        destination[pos++] = (byte) ((v >>> BYTE_SHIFT_56) & BYTE_MASK);
        destination[pos++] = (byte) ((v >>> BYTE_SHIFT_48) & BYTE_MASK);
        destination[pos++] = (byte) ((v >>> BYTE_SHIFT_40) & BYTE_MASK);
        destination[pos++] = (byte) ((v >>> BYTE_SHIFT_32) & BYTE_MASK);
        destination[pos++] = (byte) ((v >>> BYTE_SHIFT_24) & BYTE_MASK);
        destination[pos++] = (byte) ((v >>> BYTE_SHIFT_16) & BYTE_MASK);
        destination[pos++] = (byte) ((v >>> BYTE_SHIFT_8) & BYTE_MASK);
        destination[pos] = (byte) ((v >>> BYTE_SHIFT_0) & BYTE_MASK);
    }

    private Long load(final byte[] data, final int from) {
        int pos = from;
        return ((data[pos++] & BYTE_MASK) << BYTE_SHIFT_56) |
        /**/
                ((data[pos++] & BYTE_MASK) << BYTE_SHIFT_48) |
                /**/
                ((data[pos++] & BYTE_MASK) << BYTE_SHIFT_40) |
                /**/
                ((data[pos++] & BYTE_MASK) << BYTE_SHIFT_32) |
                /**/
                ((data[pos++] & BYTE_MASK) << BYTE_SHIFT_24) |
                /**/
                ((data[pos++] & BYTE_MASK) << BYTE_SHIFT_16) |
                /**/
                ((data[pos++] & BYTE_MASK) << BYTE_SHIFT_8) |
                /**/
                ((data[pos++] & BYTE_MASK) << BYTE_SHIFT_0);
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
