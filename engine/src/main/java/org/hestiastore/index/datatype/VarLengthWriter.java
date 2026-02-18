package org.hestiastore.index.datatype;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileWriter;

public class VarLengthWriter<T> implements TypeWriter<T> {

    private static final ConvertorToBytes<Integer> CONVERTOR_TO_BYTES = new TypeDescriptorInteger()
            .getConvertorToBytes();

    private final ConvertorToBytes<T> convertor;

    public VarLengthWriter(final ConvertorToBytes<T> convertor) {
        this.convertor = Vldtn.requireNonNull(convertor, "convertor");
    }

    @Override
    public int write(final FileWriter writer, final T object) {
        final byte[] out = convertor.toBytes(object);
        if (out.length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Converted type is too big");
        }
        writer.write(CONVERTOR_TO_BYTES.toBytes(out.length));
        writer.write(out);
        return 1 + out.length;
    }
}
