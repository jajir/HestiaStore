package org.hestiastore.index.datatype;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileReader;

/**
 * Reads values encoded as {@code [int length][payload bytes]}.
 *
 * @param <T> decoded value type
 */
public class VarLengthReader<T> implements TypeReader<T> {

    private static final TypeDecoder<Integer> CONVERTOR_FROM_BYTES = new TypeDescriptorInteger()
            .getTypeDecoder();

    private final TypeDecoder<T> convertor;

    /**
     * Creates a variable-length reader.
     *
     * @param convertor payload decoder
     */
    public VarLengthReader(final TypeDecoder<T> convertor) {
        this.convertor = Vldtn.requireNonNull(convertor, "convertor");
    }

    /**
     * Reads one value from reader.
     *
     * @param reader source reader
     * @return decoded value or {@code null} on end-of-stream
     */
    @Override
    public T read(final FileReader reader) {
        final byte[] lengthBytes = new byte[4];
        if (!TypeIo.readFullyOrNull(reader, lengthBytes)) {
            return null;
        }
        final int length = CONVERTOR_FROM_BYTES.decode(lengthBytes);
        if (length < 0) {
            return null;
        }
        final byte[] bytes = new byte[length];
        TypeIo.readFullyRequired(reader, bytes);
        return convertor.decode(bytes);
    }

}
