package org.hestiastore.index.datatype;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileReader;

/**
 * Reads values stored as
 * {@code [canonical unsigned LEB128 byte length][payload bytes]}.
 *
 * @param <T> decoded value type
 */
final class VarIntLengthReader<T> implements TypeReader<T> {

    private final TypeDecoder<T> convertor;

    /**
     * Creates a variable-length reader.
     *
     * @param convertor payload decoder
     */
    VarIntLengthReader(final TypeDecoder<T> convertor) {
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
        final int length = UnsignedLeb128.read(reader);
        if (length < 0) {
            return null;
        }
        if (length > UnsignedLeb128.MAX_FRAME_PAYLOAD_BYTES) {
            throw new IndexException(String.format(
                    "Encoded payload length '%s' exceeds maximum '%s'.", length,
                    UnsignedLeb128.MAX_FRAME_PAYLOAD_BYTES));
        }
        final byte[] bytes = new byte[length];
        TypeIo.readFullyRequired(reader, bytes);
        return convertor.decode(bytes);
    }
}
