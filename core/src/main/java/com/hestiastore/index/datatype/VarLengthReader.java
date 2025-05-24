package com.hestiastore.index.datatype;

import java.util.Objects;

import com.hestiastore.index.directory.FileReader;

public class VarLengthReader<T> implements TypeReader<T> {

    private final ConvertorFromBytes<T> convertor;

    public VarLengthReader(final ConvertorFromBytes<T> convertor) {
        this.convertor = Objects.requireNonNull(convertor, "Convertor is null");
    }

    @Override
    public T read(final FileReader reader) {
        int length = reader.read();
        if (length < 0) {
            return null;
        }
        if (length > 127) {
            throw new IllegalArgumentException("Converted type is too big");
        }
        byte[] bytes = new byte[length];
        reader.read(bytes);
        return convertor.fromBytes(bytes);
    }

}
