package org.hestiastore.index.datatype;

import java.util.Comparator;

import org.hestiastore.index.directory.FileReader;
import org.hestiastore.index.directory.FileWriter;

/**
 * Descriptor for {@link Double} values.
 */
public class TypeDescriptorDouble implements TypeDescriptor<Double> {

    /**
     * Tombstone value reserved for delete semantics.
     */
    public static final Double TOMBSTONE_VALUE = Double.MAX_VALUE - 1;

    /**
     * Number of bytes required to store a double value.
     */
    private static final int REQUIRED_BYTES = 8;

    private static final TypeEncoder<Double> CONVERTOR_TO_BYTES = new TypeEncoder<Double>() {
        @Override
        public int bytesLength(final Double object) {
            return REQUIRED_BYTES;
        }

        @Override
        public int toBytes(final Double object, final byte[] destination) {
            writeBytes(object, destination);
            return REQUIRED_BYTES;
        }
    };

    /**
     * Returns fixed-size encoder for doubles.
     *
     * @return encoder
     */
    @Override
    public TypeEncoder<Double> getTypeEncoder() {
        return CONVERTOR_TO_BYTES;
    }

    /**
     * Returns fixed-size decoder for doubles.
     *
     * @return decoder
     */
    @Override
    public TypeDecoder<Double> getTypeDecoder() {
        return bytes -> load(bytes, 0);
    }

    /**
     * Returns stream reader for doubles.
     *
     * @return reader
     */
    @Override
    public TypeReader<Double> getTypeReader() {
        return new TypeReader<Double>() {
            private final byte[] readBuffer = new byte[REQUIRED_BYTES];

            @Override
            public Double read(final FileReader fileReader) {
                if (!TypeIo.readFullyOrNull(fileReader, readBuffer)) {
                    return null;
                }
                return load(readBuffer, 0);
            }
        };
    }

    Double load(byte[] bytes, int offset) {
        if (bytes.length < offset + REQUIRED_BYTES) {
            throw new IllegalArgumentException(
                    "Not enough bytes to read a Float value");
        }
        long bits = ((long) (bytes[offset] & 0xFF) << 56)
                | ((long) (bytes[offset + 1] & 0xFF) << 48)
                | ((long) (bytes[offset + 2] & 0xFF) << 40)
                | ((long) (bytes[offset + 3] & 0xFF) << 32)
                | ((long) (bytes[offset + 4] & 0xFF) << 24)
                | ((long) (bytes[offset + 5] & 0xFF) << 16)
                | ((long) (bytes[offset + 6] & 0xFF) << 8)
                | (bytes[offset + 7] & 0xFF);
        return Double.longBitsToDouble(bits);
    }

    static byte[] getBytes(Double object) {
        final byte[] out = new byte[REQUIRED_BYTES];
        writeBytes(object, out);
        return out;
    }

    private static void writeBytes(final Double object,
            final byte[] destination) {
        if (destination.length < REQUIRED_BYTES) {
            throw new IllegalArgumentException(String.format(
                    "Destination buffer too small. Required '%s' but was '%s'",
                    REQUIRED_BYTES, destination.length));
        }
        if (object == null) {
            throw new IllegalArgumentException("Object can't be null");
        }
        long bits = Double.doubleToLongBits(object);
        destination[0] = (byte) (bits >> 56);
        destination[1] = (byte) (bits >> 48);
        destination[2] = (byte) (bits >> 40);
        destination[3] = (byte) (bits >> 32);
        destination[4] = (byte) (bits >> 24);
        destination[5] = (byte) (bits >> 16);
        destination[6] = (byte) (bits >> 8);
        destination[7] = (byte) bits;
    }

    /**
     * Returns comparator for double values.
     *
     * @return comparator
     */
    @Override
    public Comparator<Double> getComparator() {
        return Double::compareTo;
    }

    /**
     * Returns stream writer for doubles.
     *
     * @return writer
     */
    @Override
    public TypeWriter<Double> getTypeWriter() {
        return new TypeWriter<Double>() {
            private final byte[] payloadBytes = new byte[REQUIRED_BYTES];

            @Override
            public int write(final FileWriter writer,
                    final Double object) {
                writeBytes(object, payloadBytes);
                writer.write(payloadBytes, 0, REQUIRED_BYTES);
                return REQUIRED_BYTES;
            }
        };
    }

    /**
     * Returns tombstone marker.
     *
     * @return tombstone value
     */
    @Override
    public Double getTombstone() {
        return TOMBSTONE_VALUE;
    }
}
