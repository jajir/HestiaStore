package org.hestiastore.index.sorteddatafile;

import org.hestiastore.index.GuardedEntryWriter;
import org.hestiastore.index.GuardedWriteTransaction;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory.Access;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileWriter;

/**
 * Write transaction that buffers sorted data into a temporary file and renames
 * it on commit.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class SortedDataFileWriterTx<K, V>
        extends GuardedWriteTransaction<EntryWriter<K, V>>
        implements WriteTransaction<K, V> {

    private static final String TEMP_FILE_SUFFIX = ".tmp";
    private final String fileName;
    private final Directory directoryFacade;
    private final int diskIoBufferSize;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;

    /**
     * Creates a writer transaction for the given sorted data file.
     *
     * @param fileName required target file name
     * @param directoryFacade required directory
     * @param diskIoBufferSize buffer size used for I/O
     * @param keyTypeDescriptor key type descriptor
     * @param valueTypeDescriptor value type descriptor
     */
    public SortedDataFileWriterTx(final String fileName,
            final Directory directoryFacade, final int diskIoBufferSize,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.fileName = Vldtn.requireNonNull(fileName, "fileName");
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        this.diskIoBufferSize = diskIoBufferSize;
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
    }

    /**
     * Opens the guarded entry writer bound to a temporary file.
     *
     * @return entry writer for this transaction
     */
    @Override
    protected EntryWriter<K, V> doOpen() {
        final FileWriter fileWriter = directoryFacade.getFileWriter(
                getTempFileName(), Access.OVERWRITE, diskIoBufferSize);
        return new GuardedEntryWriter<>(new SortedDataFileWriter<>(
                valueTypeDescriptor.getTypeWriter(), fileWriter,
                keyTypeDescriptor));
    }

    /**
     * Commits by renaming the temporary file to the final file name.
     *
     * @param writer entry writer that was used for this transaction
     */
    @Override
    protected void doCommit(final EntryWriter<K, V> writer) {
        directoryFacade.renameFile(getTempFileName(), fileName);
    }

    /**
     * Returns the temporary file name used during the transaction.
     *
     * @return temp file name
     */
    private String getTempFileName() {
        return fileName + TEMP_FILE_SUFFIX;
    }
}
