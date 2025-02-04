package com.coroptis.index.sorteddatafile;

import java.util.Comparator;
import java.util.Objects;

import com.coroptis.index.datatype.ConvertorFromBytes;
import com.coroptis.index.datatype.ConvertorToBytes;
import com.coroptis.index.datatype.TypeReader;
import com.coroptis.index.datatype.TypeWriter;
import com.coroptis.index.directory.Directory;

public class SortedDataFileBuilder<K, V> {

    private final static int DELAULT_FILE_READING_BUFFER_SIZE = 1024 * 4;

    private Directory directory;

    private String fileName;

    private int diskIoBufferSize = DELAULT_FILE_READING_BUFFER_SIZE;

    private TypeWriter<V> valueWriter;

    private TypeReader<V> valueReader;

    private Comparator<K> keyComparator;

    private ConvertorFromBytes<K> keyConvertorFromBytes;

    private ConvertorToBytes<K> keyConvertorToBytes;

    public SortedDataFileBuilder<K, V> withDirectory(final Directory directory) {
        this.directory = Objects.requireNonNull(directory);
        return this;
    }

    public SortedDataFileBuilder<K, V> withFileName(final String file) {
        this.fileName = Objects.requireNonNull(file);
        return this;
    }

    public SortedDataFileBuilder<K, V> withDiskIoBufferSize(
            final int diskIoBufferSize) {
        this.diskIoBufferSize = Objects
                .requireNonNull(diskIoBufferSize);
        return this;
    }

    public SortedDataFileBuilder<K, V> withValueWriter(
            final TypeWriter<V> valueWriter) {
        this.valueWriter = Objects.requireNonNull(valueWriter);
        return this;
    }

    public SortedDataFileBuilder<K, V> withValueReader(
            final TypeReader<V> valueReader) {
        this.valueReader = Objects.requireNonNull(valueReader);
        return this;
    }

    public SortedDataFileBuilder<K, V> withKeyComparator(
            final Comparator<K> keyComparator) {
        this.keyComparator = Objects.requireNonNull(keyComparator);
        return this;
    }

    public SortedDataFileBuilder<K, V> withKeyConvertorFromBytes(
            final ConvertorFromBytes<K> keyConvertorFromBytes) {
        this.keyConvertorFromBytes = Objects
                .requireNonNull(keyConvertorFromBytes);
        return this;
    }

    public SortedDataFileBuilder<K, V> withKeyConvertorToBytes(
            final ConvertorToBytes<K> keyConvertorToBytes) {
        this.keyConvertorToBytes = Objects.requireNonNull(keyConvertorToBytes);
        return this;
    }

    public SortedDataFile<K, V> build() {
        return new SortedDataFile<>(directory, fileName, valueWriter, valueReader,
                keyComparator, keyConvertorFromBytes, keyConvertorToBytes,
                diskIoBufferSize);
    }

}
