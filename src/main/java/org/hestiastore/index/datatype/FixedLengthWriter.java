package org.hestiastore.index.datatype;

import org.hestiastore.index.ByteSequence;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileWriter;

public class FixedLengthWriter<T> implements TypeWriter<T> {

    private final ConvertorToBytes<T> convertor;

    public FixedLengthWriter(final ConvertorToBytes<T> convertor) {
        this.convertor = Vldtn.requireNonNull(convertor, "convertor");
    }

    @Override
    public int write(final FileWriter writer, final T object) {
        final ByteSequence out = convertor.toBytesBuffer(object);
        writer.write(out);
        return out.length();
    }

}
