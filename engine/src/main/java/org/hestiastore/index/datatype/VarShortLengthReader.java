package org.hestiastore.index.datatype;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileReader;

/**
 * Reads values encoded as {@code [1-byte length][payload bytes]}.
 *
 * @param <T> decoded value type
 */
public class VarShortLengthReader<T> implements TypeReader<T> {

    private final TypeDecoder<T> convertor;

    /**
     * Creates a short-length reader.
     *
     * @param convertor payload decoder
     */
    public VarShortLengthReader(final TypeDecoder<T> convertor) {
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
        final int length = reader.read();
        if (length < 0) {
            return null;
        }
        if (length > 127) {
            throw new IllegalArgumentException("Converted type is too big");
        }
        final byte[] bytes = new byte[length];
        TypeIo.readFullyRequired(reader, bytes);
        return convertor.decode(bytes);
    }

}
