package org.hestiastore.index.datatype;

import java.util.Comparator;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileReader;
import org.hestiastore.index.directory.FileWriter;

/**
 * Descriptor for {@link Integer} values.
 */
public class TypeDescriptorInteger implements TypeDescriptor<Integer> {

    /**
     * Tombstone value reserved for delete semantics.
     */
    public static final Integer TOMBSTONE_VALUE = Integer.MIN_VALUE + 1;

    /**
     * Number of bytes required to store one integer.
     */
    static final int REQUIRED_BYTES = 4;

    /**
     * Mask used to read/write individual bytes.
     */
    private static final int BYTE_MASK = 0xFF;

    private static final int BYTE_SHIFT_0 = 0;
    private static final int BYTE_SHIFT_8 = 8;
    private static final int BYTE_SHIFT_16 = 16;
    private static final int BYTE_SHIFT_24 = 24;

    private static final TypeEncoder<Integer> CONVERTOR_TO_BYTES = new TypeEncoder<Integer>() {
        @Override
        public EncodedBytes encode(final Integer object,
                final byte[] reusableBuffer) {
            final byte[] validatedBuffer = Vldtn.requireNonNull(reusableBuffer,
                    "reusableBuffer");
            byte[] output = validatedBuffer;
            if (output.length < REQUIRED_BYTES) {
                output = new byte[REQUIRED_BYTES];
            }
            writeBytes(object, output);
            return new EncodedBytes(output, REQUIRED_BYTES);
        }
    };

    /**
     * Returns fixed-size encoder for integers.
     *
     * @return encoder
     */
    @Override
    public TypeEncoder<Integer> getTypeEncoder() {
        return CONVERTOR_TO_BYTES;
    }

    /**
     * Returns fixed-size decoder for integers.
     *
     * @return decoder
     */
    @Override
    public TypeDecoder<Integer> getTypeDecoder() {
        return bytes -> load(bytes, 0);
    }

    /**
     * Returns stream reader for integers.
     *
     * @return reader
     */
    @Override
    public TypeReader<Integer> getTypeReader() {
        return new TypeReader<Integer>() {
            private final byte[] readBuffer = new byte[REQUIRED_BYTES];

            @Override
            public Integer read(final FileReader fileReader) {
                if (!TypeIo.readFullyOrNull(fileReader, readBuffer)) {
                    return null;
                }
                return load(readBuffer, 0);
            }
        };
    }

    /**
     * Returns stream writer for integers.
     *
     * @return writer
     */
    @Override
    public TypeWriter<Integer> getTypeWriter() {
        return new TypeWriter<Integer>() {
            private final byte[] payloadBytes = new byte[REQUIRED_BYTES];

            @Override
            public int write(final FileWriter writer,
                    final Integer object) {
                writeBytes(object, payloadBytes);
                writer.write(payloadBytes, 0, REQUIRED_BYTES);
                return REQUIRED_BYTES;
            }
        };
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

    /**
     * Returns comparator for integer values.
     *
     * @return comparator
     */
    @Override
    public Comparator<Integer> getComparator() {
        return (i1, i2) -> i1 - i2;
    }

    /**
     * Returns tombstone marker.
     *
     * @return tombstone value
     */
    @Override
    public Integer getTombstone() {
        return TOMBSTONE_VALUE;
    }

}
