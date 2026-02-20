package org.hestiastore.index.unsorteddatafile;

import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.GuardedEntryWriter;
import org.hestiastore.index.GuardedWriteTransaction;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.datatype.TypeWriter;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.Directory.Access;

/**
 * A transaction for writing unsorted key-value entries to a temporary file. Upon
 * commit, the temporary file is renamed to the target file name.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class UnsortedDataFileWriterTx<K, V>
        extends GuardedWriteTransaction<EntryWriter<K, V>>
        implements WriteTransaction<K, V> {

    private static final String TEMP_FILE_SUFFIX = ".tmp";
    private final String fileName;
    private final Directory directoryFacade;
    private final int diskIoBufferSize;
    private final TypeWriter<K> keyWriter;
    private final TypeWriter<V> valueWriter;

    /**
     * Constructs a new UnsortedDataFileWriterTx.
     *
     * @param fileName            required target file name
     * @param directoryFacade     required directory facade to write to
     * @param diskIoBufferSize    the size of the disk I/O buffer
     * @param keyWriter           required key writer
     * @param valueWriter         required value writer
     */
    public UnsortedDataFileWriterTx(final String fileName,
            final Directory directoryFacade, final int diskIoBufferSize,
            final TypeWriter<K> keyWriter, final TypeWriter<V> valueWriter) {
        this.fileName = Vldtn.requireNonNull(fileName, "fileName");
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        this.diskIoBufferSize = diskIoBufferSize;
        this.keyWriter = Vldtn.requireNonNull(keyWriter, "keyWriter");
        this.valueWriter = Vldtn.requireNonNull(valueWriter, "valueWriter");
    }

    @Override
    protected EntryWriter<K, V> doOpen() {
        final Access access = Access.OVERWRITE;
        return new GuardedEntryWriter<>(new UnsortedDataFileWriter<>(
                directoryFacade, getTempFileName(), keyWriter, valueWriter,
                access,
                diskIoBufferSize));
    }

    @Override
    protected void doCommit(final EntryWriter<K, V> writer) {
        directoryFacade.renameFile(getTempFileName(), fileName);
    }

    private String getTempFileName() {
        return fileName + TEMP_FILE_SUFFIX;
    }
}
