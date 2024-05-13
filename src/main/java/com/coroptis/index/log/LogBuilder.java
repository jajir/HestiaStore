package com.coroptis.index.log;

import java.util.Objects;

import com.coroptis.index.datatype.TypeReader;
import com.coroptis.index.datatype.TypeWriter;
import com.coroptis.index.directory.Directory;

public class LogBuilder<K, V> {

    private Directory directory;

    private String fileName;

    private TypeWriter<K> keyWriter;

    private TypeWriter<V> valueWriter;

    private TypeReader<K> keyReader;

    private TypeReader<V> valueReader;

    public LogBuilder<K, V> withDirectory(final Directory directory) {
        this.directory = Objects.requireNonNull(directory);
        return this;
    }

    public LogBuilder<K, V> withFileName(final String file) {
        this.fileName = Objects.requireNonNull(file);
        return this;
    }

    public LogBuilder<K, V> withKeyWriter(final TypeWriter<K> keyWriter) {
        this.keyWriter = Objects.requireNonNull(keyWriter);
        return this;
    }

    public LogBuilder<K, V> withValueWriter(final TypeWriter<V> valueWriter) {
        this.valueWriter = Objects.requireNonNull(valueWriter);
        return this;
    }

    public LogBuilder<K, V> withKeyReader(final TypeReader<K> keyReader) {
        this.keyReader = Objects.requireNonNull(keyReader);
        return this;
    }

    public LogBuilder<K, V> withValueReader(final TypeReader<V> valueReader) {
        this.valueReader = Objects.requireNonNull(valueReader);
        return this;
    }

    public LogImpl<K, V> build() {
        return new LogImpl<>(directory, fileName, keyWriter, valueWriter, keyReader,
                valueReader);
    }
}
