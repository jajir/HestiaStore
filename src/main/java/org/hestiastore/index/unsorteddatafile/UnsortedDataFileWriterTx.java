package org.hestiastore.index.unsorteddatafile;

import org.hestiastore.index.PairWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.datatype.TypeWriter;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.Directory.Access;

/**
 * A transaction for writing unsorted key-value pairs to a temporary file. Upon
 * commit, the temporary file is renamed to the target file name.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class UnsortedDataFileWriterTx<K, V> implements WriteTransaction<K, V> {

    private static final String TEMP_FILE_SUFFIX = ".tmp";
    private final String fileName;
    private final Directory directory;
    private final int diskIoBufferSize;
    private final TypeWriter<K> keyWriter;
    private final TypeWriter<V> valueWriter;

    /**
     * Constructs a new UnsortedDataFileWriterTx.
     *
     * @param fileName            required target file name
     * @param directory           required directory to write to
     * @param diskIoBufferSize    the size of the disk I/O buffer
     * @param keyTypeDescriptor   required key type descriptor
     * @param valueTypeDescriptor required value type descriptor
     */
    public UnsortedDataFileWriterTx(final String fileName,
            final Directory directory, final int diskIoBufferSize,
            final TypeWriter<K> keyWriter, final TypeWriter<V> valueWriter) {
        this.fileName = Vldtn.requireNonNull(fileName, "fileName");
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.diskIoBufferSize = diskIoBufferSize;

        this.keyWriter = Vldtn.requireNonNull(keyWriter, "keyWriter");
        this.valueWriter = Vldtn.requireNonNull(valueWriter, "valueWriter");
    }

    /**
     * Opens a UnsortedDataFileWriter to write unsorted key-value pairs to the
     * temporary file.
     *
     * @return a UnsortedDataFileWriter instance
     */
    @Override
    public PairWriter<K, V> openWriter() {
        return openWriter(Access.OVERWRITE);
    }

    /**
     * Opens a UnsortedDataFileWriter to write unsorted key-value pairs to the
     * temporary file.
     *
     * @param access required file access mode
     * @return a UnsortedDataFileWriter instance
     */
    public PairWriter<K, V> openWriter(final Access access) {
        Vldtn.requireNonNull(access, "access");
        Access used = null;
        if (directory.isFileExists(fileName)) {
            used = access;
        } else {
            used = Access.OVERWRITE;
        }
        return new UnsortedDataFileWriter<>(directory, fileName, keyWriter,
                valueWriter, used, diskIoBufferSize);
    }

    @Override
    public void commit() {
        directory.renameFile(getTempFileName(), fileName);
    }

    private String getTempFileName() {
        return fileName + TEMP_FILE_SUFFIX;
    }

}
