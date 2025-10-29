package org.hestiastore.index.datatype;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileWriter;

public class FixedLengthWriter<T> implements TypeWriter<T> {

    private final ConvertorToBytes<T> convertor;

    public FixedLengthWriter(final ConvertorToBytes<T> convertor) {
        this.convertor = Vldtn.requireNonNull(convertor, "convertor");
    }

    @Override
    public int write(final FileWriter writer, final T object) {
        final Bytes out = convertor.toBytesBuffer(object);
        writer.write(out);
        return out.length();
    }

}
