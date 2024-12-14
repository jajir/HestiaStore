package com.coroptis.index.unsorteddatafile;

import java.util.Objects;

import com.coroptis.index.datatype.TypeReader;
import com.coroptis.index.datatype.TypeWriter;
import com.coroptis.index.directory.Directory;

public class UnsortedDataFileBuilder<K, V> {

    private Directory directory;

    private String fileName;

    private TypeWriter<K> keyWriter;

    private TypeWriter<V> valueWriter;

    private TypeReader<K> keyReader;

    private TypeReader<V> valueReader;

    public UnsortedDataFileBuilder<K, V> withDirectory(
            final Directory directory) {
        this.directory = Objects.requireNonNull(directory);
        return this;
    }

    public UnsortedDataFileBuilder<K, V> withFileName(final String file) {
        this.fileName = Objects.requireNonNull(file);
        return this;
    }

    public UnsortedDataFileBuilder<K, V> withKeyWriter(
            final TypeWriter<K> keyWriter) {
        this.keyWriter = Objects.requireNonNull(keyWriter);
        return this;
    }

    public UnsortedDataFileBuilder<K, V> withValueWriter(
            final TypeWriter<V> valueWriter) {
        this.valueWriter = Objects.requireNonNull(valueWriter);
        return this;
    }

    public UnsortedDataFileBuilder<K, V> withKeyReader(
            final TypeReader<K> keyReader) {
        this.keyReader = Objects.requireNonNull(keyReader);
        return this;
    }

    public UnsortedDataFileBuilder<K, V> withValueReader(
            final TypeReader<V> valueReader) {
        this.valueReader = Objects.requireNonNull(valueReader);
        return this;
    }

    public UnsortedDataFile<K, V> build() {
        return new UnsortedDataFile<>(directory, fileName, keyWriter,
                valueWriter, keyReader, valueReader);
    }

}
