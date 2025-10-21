package org.hestiastore.index.unsorteddatafile;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeReader;
import org.hestiastore.index.datatype.TypeWriter;
import org.hestiastore.index.directory.Directory;

/**
 * Fluent builder for creating {@link UnsortedDataFile} instances backed by a
 * {@link Directory}.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class UnsortedDataFileBuilder<K, V> {

    private static final int DEFAULT_DISK_IO_BUFFER_SIZE = 4 * 1024;

    private Directory directory;
    private String fileName;
    private TypeWriter<K> keyWriter;
    private TypeWriter<V> valueWriter;
    private TypeReader<K> keyReader;
    private TypeReader<V> valueReader;
    private int diskIoBufferSize = DEFAULT_DISK_IO_BUFFER_SIZE;

    /**
     * Sets the directory that will host the data file.
     *
     * @param directory backing directory
     * @return this builder for method chaining
     */
    public UnsortedDataFileBuilder<K, V> withDirectory(
            final Directory directory) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        return this;
    }

    /**
     * Sets the physical file name used for storage.
     *
     * @param fileName target file name
     * @return this builder for method chaining
     */
    public UnsortedDataFileBuilder<K, V> withFileName(final String fileName) {
        this.fileName = Vldtn.requireNonNull(fileName, "fileName");
        return this;
    }

    /**
     * Configures the writer responsible for serialising keys.
     *
     * @param keyWriter key writer implementation
     * @return this builder for method chaining
     */
    public UnsortedDataFileBuilder<K, V> withKeyWriter(
            final TypeWriter<K> keyWriter) {
        this.keyWriter = Vldtn.requireNonNull(keyWriter, "keyWriter");
        return this;
    }

    /**
     * Configures the writer responsible for serialising values.
     *
     * @param valueWriter value writer implementation
     * @return this builder for method chaining
     */
    public UnsortedDataFileBuilder<K, V> withValueWriter(
            final TypeWriter<V> valueWriter) {
        this.valueWriter = Vldtn.requireNonNull(valueWriter, "valueWriter");
        return this;
    }

    /**
     * Configures the reader used to deserialize keys.
     *
     * @param keyReader key reader implementation
     * @return this builder for method chaining
     */
    public UnsortedDataFileBuilder<K, V> withKeyReader(
            final TypeReader<K> keyReader) {
        this.keyReader = Vldtn.requireNonNull(keyReader, "keyReader");
        return this;
    }

    /**
     * Configures the reader used to deserialize values.
     *
     * @param valueReader value reader implementation
     * @return this builder for method chaining
     */
    public UnsortedDataFileBuilder<K, V> withValueReader(
            final TypeReader<V> valueReader) {
        this.valueReader = Vldtn.requireNonNull(valueReader, "valueReader");
        return this;
    }

    /**
     * Overrides the disk I/O buffer size used by readers and writers.
     *
     * @param diskIoBufferSize buffer size in bytes
     * @return this builder for method chaining
     */
    public UnsortedDataFileBuilder<K, V> withDiskIoBufferSize(
            final int diskIoBufferSize) {
        this.diskIoBufferSize = diskIoBufferSize;
        return this;
    }

    /**
     * Builds a new {@link UnsortedDataFile} configured with the provided
     * components.
     *
     * @return configured data file instance
     */
    public UnsortedDataFile<K, V> build() {
        return new UnsortedDataFileImpl<>(directory, fileName, keyWriter,
                valueWriter, keyReader, valueReader, diskIoBufferSize);
    }

}
