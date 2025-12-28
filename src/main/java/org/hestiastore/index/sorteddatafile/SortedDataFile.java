package org.hestiastore.index.sorteddatafile;

import org.hestiastore.index.EntryIteratorWithCurrent;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.DirectoryFacade;
import org.hestiastore.index.directory.async.AsyncFileReaderBlockingAdapter;
import org.hestiastore.index.unsorteddatafile.DataFileIterator;

public class SortedDataFile<K, V> {

    private final DirectoryFacade directoryFacade;

    private final String fileName;

    private final int diskIoBufferSize;

    private final TypeDescriptor<K> keyTypeDescriptor;

    private final TypeDescriptor<V> valueTypeDescriptor;

    public static <M, N> SortedDataFileBuilder<M, N> builder() {
        return new SortedDataFileBuilder<M, N>();
    }

    public SortedDataFile(final DirectoryFacade directoryFacade,
            final String fileName, final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final int diskIoBufferSize) {
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
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
        return new SortedDataFile<>(directoryFacade, newFileName,
                keyTypeDescriptor, valueTypeDescriptor, diskIoBufferSize);
    }

    public static <K, V> SortedDataFile<K, V> fromDirectoryFacade(
            final DirectoryFacade directoryFacade, final String fileName,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final int diskIoBufferSize) {
        return new SortedDataFile<>(directoryFacade, fileName,
                keyTypeDescriptor, valueTypeDescriptor, diskIoBufferSize);
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
    public SortedDataFile<K, V> withProperties(
            final DirectoryFacade newDirectoryFacade, final String newFileName,
            final int newDiskIoBufferSize) {
        return new SortedDataFile<>(newDirectoryFacade, newFileName,
                keyTypeDescriptor, valueTypeDescriptor, newDiskIoBufferSize);
    }

    /**
     * Opens an iterator for the sorted data file.
     * 
     * @return a entry iterator for the sorted data file
     */
    public EntryIteratorWithCurrent<K, V> openIterator() {
        if (!directoryFacade.isFileExistsAsync(fileName)
                .toCompletableFuture().join()) {
            return new EmptyEntryIteratorWithCurrent<>();
        }
        final DiffKeyReader<K> diffKeyReader = new DiffKeyReader<>(
                keyTypeDescriptor.getConvertorFromBytes());
        return new DataFileIterator<>(diffKeyReader,
                valueTypeDescriptor.getTypeReader(),
                new AsyncFileReaderBlockingAdapter(
                        directoryFacade
                                .getFileReaderAsync(fileName,
                                        diskIoBufferSize)
                                .toCompletableFuture().join()));
    }

    /**
     * Opens a writer for the sorted data file. If the file already exists, it
     * will be overwritten.
     * 
     * @return a writer for the sorted data file
     */
    public SortedDataFileWriterTx<K, V> openWriterTx() {
        return new SortedDataFileWriterTx<>(fileName, directoryFacade,
                diskIoBufferSize, keyTypeDescriptor, valueTypeDescriptor);
    }

    /**
     * Deletes the underlying file. Does nothing if the file does not exist.
     */
    public void delete() {
        directoryFacade.deleteFileAsync(fileName).toCompletableFuture().join();
    }

}
