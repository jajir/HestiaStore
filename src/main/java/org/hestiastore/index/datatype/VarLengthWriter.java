package org.hestiastore.index.datatype;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileWriter;

public class VarLengthWriter<T> implements TypeWriter<T> {

    private final ConvertorToBytes<T> convertor;

    public VarLengthWriter(final ConvertorToBytes<T> convertor) {
        this.convertor = Vldtn.requireNonNull(convertor, "convertor");
    }

    @Override
    public int write(final FileWriter writer, final T object) {
        final byte[] out = convertor.toBytes(object);
        if (out.length > 127) {
            throw new IllegalArgumentException("Converted type is too big");
        }
        writer.write((byte) out.length);
        writer.write(out);
        return 1 + out.length;
    }

}
