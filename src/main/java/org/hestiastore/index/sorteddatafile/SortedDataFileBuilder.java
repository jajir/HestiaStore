package org.hestiastore.index.sorteddatafile;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;

public class SortedDataFileBuilder<K, V> {

    private static final int DELAULT_FILE_READING_BUFFER_SIZE = 1024 * 4;

    private Directory directory;

    private String fileName;

    private int diskIoBufferSize = DELAULT_FILE_READING_BUFFER_SIZE;

    private TypeDescriptor<K> keyTypeDescriptor;

    private TypeDescriptor<V> valueTypeDescriptor;

    public SortedDataFileBuilder<K, V> withDirectory(
            final Directory directory) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        return this;
    }

    public SortedDataFileBuilder<K, V> withFileName(final String fileName) {
        this.fileName = Vldtn.requireNonNull(fileName, "fileName");
        return this;
    }

    public SortedDataFileBuilder<K, V> withDiskIoBufferSize(
            final int diskIoBufferSize) {
        this.diskIoBufferSize = Vldtn.requireNonNull(diskIoBufferSize,
                "diskIoBufferSize");
        return this;
    }

    public SortedDataFileBuilder<K, V> withKeyTypeDescriptor(
            final TypeDescriptor<K> keyTypeDescriptor) {
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        return this;
    }

    public SortedDataFileBuilder<K, V> withValueTypeDescriptor(
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        return this;
    }

    public SortedDataFile<K, V> build() {
        return new SortedDataFile<>(directory, fileName, keyTypeDescriptor,
                valueTypeDescriptor, diskIoBufferSize);
    }

}
