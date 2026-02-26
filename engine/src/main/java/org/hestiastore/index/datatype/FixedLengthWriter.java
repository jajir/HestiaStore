package org.hestiastore.index.datatype;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileWriter;

public class FixedLengthWriter<T> implements TypeWriter<T> {

    private final TypeEncoder<T> convertor;

    public FixedLengthWriter(final TypeEncoder<T> convertor) {
        this.convertor = Vldtn.requireNonNull(convertor, "convertor");
    }

    @Override
    public int write(final FileWriter writer, final T object) {
        final byte[] out = TypeEncoder.toByteArray(convertor, object);
        writer.write(out);
        return out.length;
    }

}
