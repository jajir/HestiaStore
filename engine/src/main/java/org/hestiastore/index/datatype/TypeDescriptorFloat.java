package org.hestiastore.index.datatype;

import java.util.Comparator;

/**
 * Descriptor for {@link Float} values.
 */
public class TypeDescriptorFloat implements TypeDescriptor<Float> {

    /**
     * Tombstone value reserved for delete semantics.
     */
    public static final Float TOMBSTONE_VALUE = Float.MAX_VALUE - 1;

    /**
     * Number of bytes required to store a float value.
     */
    private static final int REQUIRED_BYTES = 4;

    private static final TypeEncoder<Float> CONVERTOR_TO_BYTES = new TypeEncoder<Float>() {
        @Override
        public int bytesLength(final Float object) {
            return REQUIRED_BYTES;
        }

        @Override
        public int toBytes(final Float object, final byte[] destination) {
            writeBytes(object, destination);
            return REQUIRED_BYTES;
        }
    };

    /**
     * Returns fixed-size encoder for floats.
     *
     * @return encoder
     */
    @Override
    public TypeEncoder<Float> getTypeEncoder() {
        return CONVERTOR_TO_BYTES;
    }

    /**
     * Returns fixed-size decoder for floats.
     *
     * @return decoder
     */
    @Override
    public TypeDecoder<Float> getTypeDecoder() {
        return bytes -> load(bytes, 0);
    }

    /**
     * Returns stream reader for floats.
     *
     * @return reader
     */
    @Override
    public TypeReader<Float> getTypeReader() {
        return fileReader -> {
            final byte[] bytes = new byte[REQUIRED_BYTES];
            if (!TypeIo.readFullyOrNull(fileReader, bytes)) {
                return null;
            }
            return load(bytes, 0);
        };
    }

    Float load(byte[] bytes, int offset) {
        if (bytes.length < offset + REQUIRED_BYTES) {
            throw new IllegalArgumentException(
                    "Not enough bytes to read a Float value");
        }
        int bits = ((bytes[offset] & 0xFF) << 24)
                | ((bytes[offset + 1] & 0xFF) << 16)
                | ((bytes[offset + 2] & 0xFF) << 8)
                | (bytes[offset + 3] & 0xFF);
        return Float.intBitsToFloat(bits);
    }

    static byte[] getBytes(Float object) {
        final byte[] out = new byte[REQUIRED_BYTES];
        writeBytes(object, out);
        return out;
    }

    private static void writeBytes(final Float object,
            final byte[] destination) {
        if (destination.length < REQUIRED_BYTES) {
            throw new IllegalArgumentException(String.format(
                    "Destination buffer too small. Required '%s' but was '%s'",
                    REQUIRED_BYTES, destination.length));
        }
        if (object == null) {
            throw new IllegalArgumentException("Object can't be null");
        }
        int bits = Float.floatToIntBits(object);
        destination[0] = (byte) (bits >> 24);
        destination[1] = (byte) (bits >> 16);
        destination[2] = (byte) (bits >> 8);
        destination[3] = (byte) bits;
    }

    /**
     * Returns comparator for float values.
     *
     * @return comparator
     */
    @Override
    public Comparator<Float> getComparator() {
        return Float::compareTo;
    }

    /**
     * Returns stream writer for floats.
     *
     * @return writer
     */
    @Override
    public TypeWriter<Float> getTypeWriter() {
        return (writer, object) -> {
            writer.write(getBytes(object));
            return REQUIRED_BYTES;
        };
    }

    /**
     * Returns tombstone marker.
     *
     * @return tombstone value
     */
    @Override
    public Float getTombstone() {
        return TOMBSTONE_VALUE;
    }

}
