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
        byte[] lengthBytes = new byte[4];
        reader.read(lengthBytes);
        int length = CONVERTOR_FROM_BYTES.decode(lengthBytes);
        if (length < 0) {
            return null;
        }
        if (length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Converted type is too big");
        }
        byte[] bytes = new byte[length];
        reader.read(bytes);
        return convertor.decode(bytes);
    }

}
