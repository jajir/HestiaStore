package org.hestiastore.index.datatype;

import org.hestiastore.index.ByteSequence;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileWriter;

public class VarShortLengthWriter<T> implements TypeWriter<T> {

    private final ConvertorToBytes<T> convertor;

    public VarShortLengthWriter(final ConvertorToBytes<T> convertor) {
        this.convertor = Vldtn.requireNonNull(convertor, "convertor");
    }

    @Override
    public int write(final FileWriter writer, final T object) {
        final ByteSequence out = convertor.toBytesBuffer(object);
        if (out.length() > 127) {
            throw new IllegalArgumentException("Converted type is too big");
        }
        writer.write((byte) out.length());
        writer.write(out);
        return 1 + out.length();
    }

}
