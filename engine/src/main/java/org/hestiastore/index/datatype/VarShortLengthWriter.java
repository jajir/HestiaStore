package org.hestiastore.index.datatype;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileWriter;

public class VarShortLengthWriter<T> implements TypeWriter<T> {

    private final TypeEncoder<T> convertor;

    public VarShortLengthWriter(final TypeEncoder<T> convertor) {
        this.convertor = Vldtn.requireNonNull(convertor, "convertor");
    }

    @Override
    public int write(final FileWriter writer, final T object) {
        final byte[] out = TypeEncoder.toByteArray(convertor, object);
        if (out.length > 127) {
            throw new IllegalArgumentException("Converted type is too big");
        }
        writer.write((byte) out.length);
        writer.write(out);
        return 1 + out.length;
    }

}
