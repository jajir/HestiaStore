package org.hestiastore.index.datatype;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileReader;

/**
 * Reads values stored as {@code [unsigned byte length][payload bytes]}.
 *
 * @param <T> decoded value type
 */
final class UnsignedByteLengthReader<T> implements TypeReader<T> {

    private final TypeDecoder<T> convertor;

    /**
     * Creates an unsigned-byte-length reader.
     *
     * @param convertor payload decoder
     */
    UnsignedByteLengthReader(final TypeDecoder<T> convertor) {
        this.convertor = Vldtn.requireNonNull(convertor, "convertor");
    }

    /**
     * Reads and decodes one value.
     *
     * @param reader source reader
     * @return decoded value, or {@code null} on EOF before a length prefix
     */
    @Override
    public T read(final FileReader reader) {
        final FileReader validatedReader = Vldtn.requireNonNull(reader,
                "reader");
        final int length = validatedReader.read();
        if (length < 0) {
            return null;
        }
        final byte[] bytes = new byte[length];
        TypeIo.readFullyRequired(validatedReader, bytes);
        return convertor.decode(bytes);
    }
}
