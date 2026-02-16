package org.hestiastore.index.sorteddatafile;

import org.hestiastore.index.EntryIteratorWithCurrent;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.unsorteddatafile.DataFileIterator;

/**
 * Accessor for a sorted on-disk data file storing {@code K -> V} entries.
 *
 * <p>
 * Readers iterate entries in key order, while writers are provided through
 * {@link SortedDataFileWriterTx}. The file lives in a {@link Directory}
 * and uses the supplied type descriptors for encoding and decoding keys and
 * values.
 * </p>
 *
 * @param <K> key type
 * @param <V> value type
 */
public class SortedDataFile<K, V> {

    private final Directory directoryFacade;

    private final String fileName;

    private final int diskIoBufferSize;

    private final TypeDescriptor<K> keyTypeDescriptor;

    private final TypeDescriptor<V> valueTypeDescriptor;

    /**
     * Creates a builder for a sorted data file.
     *
     * @param <M> key type
     * @param <N> value type
     * @return builder for a sorted data file
     */
    public static <M, N> SortedDataFileBuilder<M, N> builder() {
        return new SortedDataFileBuilder<M, N>();
    }

    /**
     * Creates a sorted data file accessor with fixed storage properties.
     *
     * @param directoryFacade required directory
     * @param fileName required file name
     * @param keyTypeDescriptor key type descriptor
     * @param valueTypeDescriptor value type descriptor
     * @param diskIoBufferSize buffer size used for I/O
     */
    public SortedDataFile(final Directory directoryFacade,
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

    /**
     * Creates a new accessor bound to the given directory.
     *
     * @param directoryFacade required directory
     * @param fileName required file name
     * @param keyTypeDescriptor key type descriptor
     * @param valueTypeDescriptor value type descriptor
     * @param diskIoBufferSize buffer size used for I/O
     * @param <K> key type
     * @param <V> value type
     * @return sorted data file accessor
     */
    public static <K, V> SortedDataFile<K, V> fromDirectory(
            final Directory directoryFacade, final String fileName,
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
     * @param newDirectory   required new directory
     * @param newFileName         required new file name
     * @param newDiskIoBufferSize the new disk I/O buffer size
     * @return a new instance with the specified properties
     */
    public SortedDataFile<K, V> withProperties(
            final Directory newDirectory, final String newFileName,
            final int newDiskIoBufferSize) {
        return new SortedDataFile<>(newDirectory, newFileName,
                keyTypeDescriptor, valueTypeDescriptor, newDiskIoBufferSize);
    }

    /**
     * Opens an iterator for the sorted data file.
     * 
     * @return a entry iterator for the sorted data file
     */
    public EntryIteratorWithCurrent<K, V> openIterator() {
        if (!directoryFacade.isFileExists(fileName)) {
            return new EmptyEntryIteratorWithCurrent<>();
        }
        try {
            final DiffKeyReader<K> diffKeyReader = new DiffKeyReader<>(
                    keyTypeDescriptor.getConvertorFromBytes());
            return new DataFileIterator<>(diffKeyReader,
                    valueTypeDescriptor.getTypeReader(),
                    directoryFacade.getFileReader(fileName, diskIoBufferSize));
        } catch (final RuntimeException e) {
            if (!directoryFacade.isFileExists(fileName)) {
                return new EmptyEntryIteratorWithCurrent<>();
            }
            throw e;
        }
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
        directoryFacade.deleteFile(fileName);
    }

}
