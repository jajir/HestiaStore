package org.hestiastore.index.sorteddatafile;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;

/**
 * Builder for {@link SortedDataFile} instances.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class SortedDataFileBuilder<K, V> {

    private static final int DELAULT_FILE_READING_BUFFER_SIZE = 1024 * 4;

    private Directory directoryFacade;

    private String fileName;

    private int diskIoBufferSize = DELAULT_FILE_READING_BUFFER_SIZE;

    private TypeDescriptor<K> keyTypeDescriptor;

    private TypeDescriptor<V> valueTypeDescriptor;

    /**
     * Sets the directory used to access the file.
     *
     * @param directoryFacade required directory
     * @return this builder
     */
    public SortedDataFileBuilder<K, V> withDirectory(
            final Directory directoryFacade) {
        Vldtn.requireNonNull(directoryFacade, "directoryFacade");
        this.directoryFacade = directoryFacade;
        return this;
    }

    /**
     * Sets the target file name.
     *
     * @param fileName required file name
     * @return this builder
     */
    public SortedDataFileBuilder<K, V> withFileName(final String fileName) {
        this.fileName = Vldtn.requireNonNull(fileName, "fileName");
        return this;
    }

    /**
     * Sets the I/O buffer size used for reads and writes.
     *
     * @param diskIoBufferSize required buffer size
     * @return this builder
     */
    public SortedDataFileBuilder<K, V> withDiskIoBufferSize(
            final int diskIoBufferSize) {
        this.diskIoBufferSize = Vldtn.requireNonNull(diskIoBufferSize,
                "diskIoBufferSize");
        return this;
    }

    /**
     * Sets the key type descriptor.
     *
     * @param keyTypeDescriptor required key descriptor
     * @return this builder
     */
    public SortedDataFileBuilder<K, V> withKeyTypeDescriptor(
            final TypeDescriptor<K> keyTypeDescriptor) {
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        return this;
    }

    /**
     * Sets the value type descriptor.
     *
     * @param valueTypeDescriptor required value descriptor
     * @return this builder
     */
    public SortedDataFileBuilder<K, V> withValueTypeDescriptor(
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        return this;
    }

    /**
     * Builds a {@link SortedDataFile} using the configured properties.
     *
     * @return sorted data file accessor
     */
    public SortedDataFile<K, V> build() {
        if (directoryFacade == null) {
            throw new IllegalStateException("Directory must be provided");
        }
        return new SortedDataFile<>(directoryFacade, fileName,
                keyTypeDescriptor, valueTypeDescriptor, diskIoBufferSize);
    }

}
