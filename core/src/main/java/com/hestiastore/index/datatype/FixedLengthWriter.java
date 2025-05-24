package com.hestiastore.index.datatype;

import java.util.Objects;

import com.hestiastore.index.directory.FileWriter;

public class FixedLengthWriter<T> implements TypeWriter<T> {

    private final ConvertorToBytes<T> convertor;

    public FixedLengthWriter(final ConvertorToBytes<T> convertor) {
        this.convertor = Objects.requireNonNull(convertor, "Convertor is null");
    }

    @Override
    public int write(final FileWriter writer, final T object) {
        final byte[] out = convertor.toBytes(object);
        writer.write(out);
        return out.length;
    }

}
