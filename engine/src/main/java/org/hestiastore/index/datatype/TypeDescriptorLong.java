package org.hestiastore.index.datatype;

import java.util.Comparator;

/**
 * Descriptor for {@link Long} values.
 */
public class TypeDescriptorLong implements TypeDescriptor<Long> {

    /**
     * Tombstone value reserved for delete semantics.
     */
    public static final Long TOMBSTONE_VALUE = Long.MIN_VALUE + 1;

    /**
     * Number of bytes required to store one long value.
     */
    static final int REQUIRED_BYTES = 8;

    /**
     * Mask used to read/write individual bytes.
     */
    private static final long BYTE_MASK = 0xFFL;

    private static final int BYTE_SHIFT_0 = 0;
    private static final int BYTE_SHIFT_8 = 8;
    private static final int BYTE_SHIFT_16 = 16;
    private static final int BYTE_SHIFT_24 = 24;
    private static final int BYTE_SHIFT_32 = 32;
    private static final int BYTE_SHIFT_40 = 40;
    private static final int BYTE_SHIFT_48 = 48;
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

    /**
     * Returns fixed-size encoder for long values.
     *
     * @return encoder
     */
    @Override
    public TypeEncoder<Long> getTypeEncoder() {
        return CONVERTOR_TO_BYTES;
    }

    /**
     * Returns fixed-size decoder for long values.
     *
     * @return decoder
     */
    @Override
    public TypeDecoder<Long> getTypeDecoder() {
        return bytes -> load(bytes, 0);
    }

    /**
     * Returns stream reader for long values.
     *
     * @return reader
     */
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

    /**
     * Returns stream writer for long values.
     *
     * @return writer
     */
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
        return ((data[pos++] & BYTE_MASK) << BYTE_SHIFT_56)
                | ((data[pos++] & BYTE_MASK) << BYTE_SHIFT_48)
                | ((data[pos++] & BYTE_MASK) << BYTE_SHIFT_40)
                | ((data[pos++] & BYTE_MASK) << BYTE_SHIFT_32)
                | ((data[pos++] & BYTE_MASK) << BYTE_SHIFT_24)
                | ((data[pos++] & BYTE_MASK) << BYTE_SHIFT_16)
                | ((data[pos++] & BYTE_MASK) << BYTE_SHIFT_8)
                | ((data[pos++] & BYTE_MASK) << BYTE_SHIFT_0);
    }

    /**
     * Returns comparator for long values.
     *
     * @return comparator
     */
    @Override
    public Comparator<Long> getComparator() {
        return (i1, i2) -> i1.compareTo(i2);
    }

    /**
     * Returns tombstone marker.
     *
     * @return tombstone value
     */
    @Override
    public Long getTombstone() {
        return TOMBSTONE_VALUE;
    }

    /**
     * Returns whether object is the same descriptor type.
     *
     * @param obj object to compare
     * @return {@code true} when same descriptor type
     */
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        return obj != null && obj.getClass() == getClass();
    }

    /**
     * Returns stable hash code for descriptor type identity.
     *
     * @return hash code
     */
    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

}
