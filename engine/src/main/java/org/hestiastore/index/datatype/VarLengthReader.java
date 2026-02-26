package org.hestiastore.index.datatype;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileReader;

public class VarLengthReader<T> implements TypeReader<T> {

    private static final TypeDecoder<Integer> CONVERTOR_FROM_BYTES = new TypeDescriptorInteger()
            .getTypeDecoder();

    private final TypeDecoder<T> convertor;

    public VarLengthReader(final TypeDecoder<T> convertor) {
        this.convertor = Vldtn.requireNonNull(convertor, "convertor");
    }

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
