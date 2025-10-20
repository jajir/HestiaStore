package org.hestiastore.index.unsorteddatafile;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeReader;
import org.hestiastore.index.datatype.TypeWriter;
import org.hestiastore.index.directory.Directory;

public class UnsortedDataFileBuilder<K, V> {

    private static final int DEFAULT_DISK_IO_BUFFER_SIZE = 4 * 1024;

    private Directory directory;

    private String fileName;

    private TypeWriter<K> keyWriter;

    private TypeWriter<V> valueWriter;

    private TypeReader<K> keyReader;

    private TypeReader<V> valueReader;

    private int diskIoBufferSize = DEFAULT_DISK_IO_BUFFER_SIZE;

    public UnsortedDataFileBuilder<K, V> withDirectory(
            final Directory directory) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        return this;
    }

    public UnsortedDataFileBuilder<K, V> withFileName(final String fileName) {
        this.fileName = Vldtn.requireNonNull(fileName, "fileName");
        return this;
    }

    public UnsortedDataFileBuilder<K, V> withKeyWriter(
            final TypeWriter<K> keyWriter) {
        this.keyWriter = Vldtn.requireNonNull(keyWriter, "keyWriter");
        return this;
    }

    public UnsortedDataFileBuilder<K, V> withValueWriter(
            final TypeWriter<V> valueWriter) {
        this.valueWriter = Vldtn.requireNonNull(valueWriter, "valueWriter");
        return this;
    }

    public UnsortedDataFileBuilder<K, V> withKeyReader(
            final TypeReader<K> keyReader) {
        this.keyReader = Vldtn.requireNonNull(keyReader, "keyReader");
        return this;
    }

    public UnsortedDataFileBuilder<K, V> withValueReader(
            final TypeReader<V> valueReader) {
        this.valueReader = Vldtn.requireNonNull(valueReader, "valueReader");
        return this;
    }

    public UnsortedDataFileBuilder<K, V> withDiskIoBufferSize(
            final int diskIoBufferSize) {
        this.diskIoBufferSize = diskIoBufferSize;
        return this;
    }

    public UnsortedDataFile<K, V> build() {
        return new UnsortedDataFileImpl<>(directory, fileName, keyWriter,
                valueWriter, keyReader, valueReader, diskIoBufferSize);
    }

}
