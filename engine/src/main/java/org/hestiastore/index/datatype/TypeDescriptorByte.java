package org.hestiastore.index.datatype;

import java.util.Comparator;

import org.hestiastore.index.Vldtn;

/**
 * Descriptor for {@link Byte} values.
 */
public class TypeDescriptorByte implements TypeDescriptor<Byte> {

    private static final int REQUIRED_BYTES = 1;

    private static final TypeEncoder<Byte> CONVERTOR_TO_BYTES = new TypeEncoder<Byte>() {
        @Override
        public EncodedBytes encode(final Byte object,
                final byte[] reusableBuffer) {
            final byte validatedValue = Vldtn.requireNonNull(object, "object");
            final byte[] validatedBuffer = Vldtn.requireNonNull(reusableBuffer,
                    "reusableBuffer");
            byte[] output = validatedBuffer;
            if (output.length < REQUIRED_BYTES) {
                output = new byte[REQUIRED_BYTES];
            }
            output[0] = validatedValue;
            return new EncodedBytes(output, REQUIRED_BYTES);
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
