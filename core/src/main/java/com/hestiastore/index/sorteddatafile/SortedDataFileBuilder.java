package com.hestiastore.index.sorteddatafile;

import java.util.Objects;

import com.hestiastore.index.datatype.TypeDescriptor;
import com.hestiastore.index.directory.Directory;

public class SortedDataFileBuilder<K, V> {

    private final static int DELAULT_FILE_READING_BUFFER_SIZE = 1024 * 4;

    private Directory directory;

    private String fileName;

    private int diskIoBufferSize = DELAULT_FILE_READING_BUFFER_SIZE;

    private TypeDescriptor<K> keyTypeDescriptor;

    private TypeDescriptor<V> valueTypeDescriptor;

    public SortedDataFileBuilder<K, V> withDirectory(
            final Directory directory) {
        this.directory = Objects.requireNonNull(directory);
        return this;
    }

    public SortedDataFileBuilder<K, V> withFileName(final String file) {
        this.fileName = Objects.requireNonNull(file);
        return this;
    }

    public SortedDataFileBuilder<K, V> withDiskIoBufferSize(
            final int diskIoBufferSize) {
        this.diskIoBufferSize = Objects.requireNonNull(diskIoBufferSize);
        return this;
    }

    public SortedDataFileBuilder<K, V> withKeyTypeDescriptor(
            final TypeDescriptor<K> keyTypeDescriptor) {
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
        return this;
    }

    public SortedDataFileBuilder<K, V> withValueTypeDescriptor(
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
        return this;
    }

    public SortedDataFile<K, V> build() {
        return new SortedDataFile<>(directory, fileName, keyTypeDescriptor,
                valueTypeDescriptor, diskIoBufferSize);
    }

}
