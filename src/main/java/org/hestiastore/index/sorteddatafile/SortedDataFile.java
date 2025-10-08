package org.hestiastore.index.sorteddatafile;

import org.hestiastore.index.PairIteratorWithCurrent;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.unsorteddatafile.DataFileIterator;

public class SortedDataFile<K, V> {

    private final Directory directory;

    private final String fileName;

    private final int diskIoBufferSize;

    private final TypeDescriptor<K> keyTypeDescriptor;

    private final TypeDescriptor<V> valueTypeDescriptor;

    public static <M, N> SortedDataFileBuilder<M, N> builder() {
        return new SortedDataFileBuilder<M, N>();
    }

    public SortedDataFile(final Directory directory, final String fileName,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final int diskIoBufferSize) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.fileName = Vldtn.requireNonNull(fileName, "fileName");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.diskIoBufferSize = Vldtn.requireIoBufferSize(diskIoBufferSize);
    }

    /**
     * Creates a new instance with the specified file name. The directory, key
     * type descriptor, value type descriptor, and disk I/O buffer size remain
     * unchanged.
     * 
     * @param newFileName required new file name
     * @return a new instance with the specified file name
     */
    public SortedDataFile<K, V> withFileName(final String newFileName) {
        return new SortedDataFile<>(directory, newFileName, keyTypeDescriptor,
                valueTypeDescriptor, diskIoBufferSize);
    }

    /**
     * Creates a new instance with the specified properties. The key and value
     * type descriptors remain unchanged.
     * 
     * @param newDirectory        required new directory
     * @param newFileName         required new file name
     * @param newDiskIoBufferSize the new disk I/O buffer size
     * @return a new instance with the specified properties
     */
    public SortedDataFile<K, V> withProperties(final Directory newDirectory,
            final String newFileName, final int newDiskIoBufferSize) {
        return new SortedDataFile<>(newDirectory, newFileName,
                keyTypeDescriptor, valueTypeDescriptor, newDiskIoBufferSize);
    }

    /**
     * Opens an iterator for the sorted data file.
     * 
     * @return a pair iterator for the sorted data file
     */
    public PairIteratorWithCurrent<K, V> openIterator() {
        if (!directory.isFileExists(fileName)) {
            return new EmptyPairIteratorWithCurrent<>();
        }
        final DiffKeyReader<K> diffKeyReader = new DiffKeyReader<>(
                keyTypeDescriptor.getConvertorFromBytes());
        return new DataFileIterator<>(diffKeyReader,
                valueTypeDescriptor.getTypeReader(),
                directory.getFileReader(fileName, diskIoBufferSize));
    }

    /**
     * Opens a writer for the sorted data file. If the file already exists, it
     * will be overwritten.
     * 
     * @return a writer for the sorted data file
     */
    public SortedDataFileWriterTx<K, V> openWriterTx() {
        return new SortedDataFileWriterTx<>(fileName, directory,
                diskIoBufferSize, keyTypeDescriptor, valueTypeDescriptor);
    }

    /**
     * Deletes the underlying file. Does nothing if the file does not exist.
     */
    public void delete() {
        directory.deleteFile(fileName);
    }

}
