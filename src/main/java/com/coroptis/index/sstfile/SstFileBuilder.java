package com.coroptis.index.sstfile;

import java.util.Comparator;
import java.util.Objects;

import com.coroptis.index.datatype.ConvertorFromBytes;
import com.coroptis.index.datatype.ConvertorToBytes;
import com.coroptis.index.datatype.TypeReader;
import com.coroptis.index.datatype.TypeWriter;
import com.coroptis.index.directory.Directory;

public class SstFileBuilder<K, V> {

    private final static int DELAULT_FILE_READING_BUFFER_SIZE = 1024 * 4;

    private Directory directory;

    private String fileName;

    private int fileReadingBufferSize = DELAULT_FILE_READING_BUFFER_SIZE;

    private TypeWriter<V> valueWriter;

    private TypeReader<V> valueReader;

    private Comparator<K> keyComparator;

    private ConvertorFromBytes<K> keyConvertorFromBytes;

    private ConvertorToBytes<K> keyConvertorToBytes;

    public SstFileBuilder<K, V> withDirectory(final Directory directory) {
        this.directory = Objects.requireNonNull(directory);
        return this;
    }

    public SstFileBuilder<K, V> withFileName(final String file) {
        this.fileName = Objects.requireNonNull(file);
        return this;
    }

    public SstFileBuilder<K, V> withFileReadingBufferSize(
            final int fileReadingBufferSize) {
        this.fileReadingBufferSize = Objects
                .requireNonNull(fileReadingBufferSize);
        return this;
    }

    public SstFileBuilder<K, V> withValueWriter(
            final TypeWriter<V> valueWriter) {
        this.valueWriter = Objects.requireNonNull(valueWriter);
        return this;
    }

    public SstFileBuilder<K, V> withValueReader(
            final TypeReader<V> valueReader) {
        this.valueReader = Objects.requireNonNull(valueReader);
        return this;
    }

    public SstFileBuilder<K, V> withKeyComparator(
            final Comparator<K> keyComparator) {
        this.keyComparator = Objects.requireNonNull(keyComparator);
        return this;
    }

    public SstFileBuilder<K, V> withKeyConvertorFromBytes(
            final ConvertorFromBytes<K> keyConvertorFromBytes) {
        this.keyConvertorFromBytes = Objects
                .requireNonNull(keyConvertorFromBytes);
        return this;
    }

    public SstFileBuilder<K, V> withKeyConvertorToBytes(
            final ConvertorToBytes<K> keyConvertorToBytes) {
        this.keyConvertorToBytes = Objects.requireNonNull(keyConvertorToBytes);
        return this;
    }

    public SstFile<K, V> build() {
        return new SstFile<>(directory, fileName, valueWriter, valueReader,
                keyComparator, keyConvertorFromBytes, keyConvertorToBytes,
                fileReadingBufferSize);
    }

}
