package org.hestiastore.index.sorteddatafile;

import org.hestiastore.index.GuardedEntryWriter;
import org.hestiastore.index.GuardedWriteTransaction;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.DirectoryFacade;
import org.hestiastore.index.directory.FileWriter;

public class SortedDataFileWriterTx<K, V>
        extends GuardedWriteTransaction<EntryWriter<K, V>>
        implements WriteTransaction<K, V> {

    private static final String TEMP_FILE_SUFFIX = ".tmp";
    private final String fileName;
    private final DirectoryFacade directoryFacade;
    private final int diskIoBufferSize;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;

    public SortedDataFileWriterTx(final String fileName,
            final DirectoryFacade directoryFacade, final int diskIoBufferSize,
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

    public SortedDataFileWriterTx(final String fileName,
            final Directory directory, final int diskIoBufferSize,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor) {
        this(fileName, DirectoryFacade.of(directory), diskIoBufferSize,
                keyTypeDescriptor, valueTypeDescriptor);
    }

    @Override
    protected EntryWriter<K, V> doOpen() {
        final FileWriter fileWriter = directoryFacade.getDirectory()
                .getFileWriter(
                getTempFileName(), Directory.Access.OVERWRITE,
                diskIoBufferSize);
        return new GuardedEntryWriter<>(new SortedDataFileWriter<>(
                valueTypeDescriptor.getTypeWriter(), fileWriter,
                keyTypeDescriptor));
    }

    @Override
    protected void doCommit(final EntryWriter<K, V> writer) {
        directoryFacade.getDirectory().renameFile(getTempFileName(), fileName);
    }

    private String getTempFileName() {
        return fileName + TEMP_FILE_SUFFIX;
    }
}
