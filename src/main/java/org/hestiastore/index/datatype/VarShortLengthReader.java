package org.hestiastore.index.datatype;

import org.hestiastore.index.bytes.MutableBytes;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileReader;

public class VarShortLengthReader<T> implements TypeReader<T> {

    private final ConvertorFromBytes<T> convertor;

    public VarShortLengthReader(final ConvertorFromBytes<T> convertor) {
        this.convertor = Vldtn.requireNonNull(convertor, "convertor");
    }

    @Override
    public T read(final FileReader reader) {
        int length = reader.read();
        if (length < 0) {
            return null;
        }
        if (length > 127) {
            throw new IllegalArgumentException("Converted type is too big");
        }
        final MutableBytes bytes = MutableBytes.allocate(length);
        reader.read(bytes);
        return convertor.fromBytes(bytes.toImmutableBytes());
    }

}
