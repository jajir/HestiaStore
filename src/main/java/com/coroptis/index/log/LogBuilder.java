package com.coroptis.index.log;

import java.util.Objects;

import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeReader;
import com.coroptis.index.datatype.TypeWriter;
import com.coroptis.index.directory.Directory;

public class LogBuilder<K, V> {

    private Directory directory;

    private String fileName;

    private TypeDescriptor<K> keyTypeDescriptor;

    private TypeWriter<V> valueWriter;

    private TypeReader<V> valueReader;

    public LogBuilder<K, V> withDirectory(final Directory directory) {
        this.directory = Objects.requireNonNull(directory);
        return this;
    }

    public LogBuilder<K, V> withFileName(final String file) {
        this.fileName = Objects.requireNonNull(file);
        return this;
    }

    public LogBuilder<K, V> withKeyTypeDescriptor(
            final TypeDescriptor<K> keyTypeDescriptor) {
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
        return this;
    }

    public LogBuilder<K, V> withValueWriter(final TypeWriter<V> valueWriter) {
        this.valueWriter = Objects.requireNonNull(valueWriter);
        return this;
    }

    public LogBuilder<K, V> withValueReader(final TypeReader<V> valueReader) {
        this.valueReader = Objects.requireNonNull(valueReader);
        return this;
    }

    public LogImpl<K, V> build() {
        return new LogImpl<>(directory, fileName, keyTypeDescriptor,
                valueWriter, valueReader);
    }

    public LogEmptyImpl<K, V> buildEmpty() {
        return new LogEmptyImpl<>();
    }

}
