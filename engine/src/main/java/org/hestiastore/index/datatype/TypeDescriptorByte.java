package org.hestiastore.index.datatype;

import java.util.Comparator;

/**
 * Descriptor for {@link Byte} values.
 */
public class TypeDescriptorByte implements TypeDescriptor<Byte> {

    private static final int REQUIRED_BYTES = 1;

    private static final TypeEncoder<Byte> CONVERTOR_TO_BYTES = new TypeEncoder<Byte>() {
        @Override
        public int bytesLength(final Byte object) {
            return REQUIRED_BYTES;
        }

        @Override
        public int toBytes(final Byte object, final byte[] destination) {
            if (destination.length < REQUIRED_BYTES) {
                throw new IllegalArgumentException(String.format(
                        "Destination buffer too small. Required '%s' but was '%s'",
                        REQUIRED_BYTES, destination.length));
            }
            destination[0] = object;
            return REQUIRED_BYTES;
        }
    };

    /**
     * Tombstone value reserved for delete semantics.
     */
    private static final Byte TOMBSTONE_VALUE = Byte.MIN_VALUE;

    /**
     * Returns byte encoder.
     *
     * @return byte encoder
     */
    @Override
    public TypeEncoder<Byte> getTypeEncoder() {
        return CONVERTOR_TO_BYTES;
    }

    /**
     * Returns byte decoder.
     *
     * @return byte decoder
     */
    @Override
    public TypeDecoder<Byte> getTypeDecoder() {
        return bytes -> bytes[0];
    }

    /**
     * Returns reader that reads one byte from input.
     *
     * @return byte reader
     */
    @Override
    public TypeReader<Byte> getTypeReader() {
        return inputStream -> {
            final int read = inputStream.read();
            if (read < 0) {
                return null;
            }
            return (byte) read;
        };
    }

    /**
     * Returns writer that writes one byte to output.
     *
     * @return byte writer
     */
    @Override
    public TypeWriter<Byte> getTypeWriter() {
        return (fileWriter, b) -> {
            fileWriter.write(b);
            return REQUIRED_BYTES;
        };
    }

    /**
     * Returns comparator for byte values.
     *
     * @return byte comparator
     */
    @Override
    public Comparator<Byte> getComparator() {
        return (i1, i2) -> i2 - i1;
    }

    /**
     * Returns tombstone marker.
     *
     * @return tombstone byte
     */
    @Override
    public Byte getTombstone() {
        return TOMBSTONE_VALUE;
    }

}
