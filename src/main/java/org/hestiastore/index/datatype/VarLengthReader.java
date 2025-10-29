package org.hestiastore.index.datatype;

import org.hestiastore.index.MutableBytes;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileReader;

public class VarLengthReader<T> implements TypeReader<T> {

    private static final ConvertorFromBytes<Integer> CONVERTOR_FROM_BYTES = new TypeDescriptorInteger()
            .getConvertorFromBytes();

    private final ConvertorFromBytes<T> convertor;

    public VarLengthReader(final ConvertorFromBytes<T> convertor) {
        this.convertor = Vldtn.requireNonNull(convertor, "convertor");
    }

    @Override
    public T read(final FileReader reader) {
        final MutableBytes lengthBytes = MutableBytes.allocate(4);
        reader.read(lengthBytes);
        int length = CONVERTOR_FROM_BYTES.fromBytes(lengthBytes.toBytes());
        if (length < 0) {
            return null;
        }
        if (length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Converted type is too big");
        }
        final MutableBytes bytes = MutableBytes.allocate(length);
        reader.read(bytes);
        return convertor.fromBytes(bytes.toBytes());
    }

}
