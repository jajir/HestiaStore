package org.hestiastore.index.datatype;

import java.util.Comparator;

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
     * Thombstone value, use can't use it.
     */
    private static final Byte TOMBSTONE_VALUE = Byte.MIN_VALUE;

    @Override
    public TypeEncoder<Byte> getTypeEncoder() {
        return CONVERTOR_TO_BYTES;
    }

    @Override
    public TypeDecoder<Byte> getTypeDecoder() {
        return bytes -> bytes[0];
    }

    @Override
    public TypeReader<Byte> getTypeReader() {
        return inputStream -> (byte) inputStream.read();
    }

    @Override
    public TypeWriter<Byte> getTypeWriter() {
        return (fileWriter, b) -> {
            fileWriter.write(b);
            return REQUIRED_BYTES;
        };
    }

    @Override
    public Comparator<Byte> getComparator() {
        return (i1, i2) -> i2 - i1;
    }

    @Override
    public Byte getTombstone() {
        return TOMBSTONE_VALUE;
    }

}
