package org.hestiastore.index.datatype;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileReader;

public class VarShortLengthReader<T> implements TypeReader<T> {

    private final TypeDecoder<T> convertor;

    public VarShortLengthReader(final TypeDecoder<T> convertor) {
        this.convertor = Vldtn.requireNonNull(convertor, "convertor");
    }

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
