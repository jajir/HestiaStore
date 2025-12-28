package org.hestiastore.index.sorteddatafile;

import org.hestiastore.index.GuardedEntryWriter;
import org.hestiastore.index.GuardedWriteTransaction;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory.Access;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.directory.FileWriter;
import org.hestiastore.index.directory.async.AsyncFileWriterBlockingAdapter;

public class SortedDataFileWriterTx<K, V>
        extends GuardedWriteTransaction<EntryWriter<K, V>>
        implements WriteTransaction<K, V> {

    private static final String TEMP_FILE_SUFFIX = ".tmp";
    private final String fileName;
    private final AsyncDirectory directoryFacade;
    private final int diskIoBufferSize;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;

    public SortedDataFileWriterTx(final String fileName,
            final AsyncDirectory directoryFacade, final int diskIoBufferSize,
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

    @Override
    protected EntryWriter<K, V> doOpen() {
        final FileWriter fileWriter = new AsyncFileWriterBlockingAdapter(
                directoryFacade
                        .getFileWriterAsync(getTempFileName(),
                                Access.OVERWRITE,
                                diskIoBufferSize)
                        .toCompletableFuture().join());
        return new GuardedEntryWriter<>(new SortedDataFileWriter<>(
                valueTypeDescriptor.getTypeWriter(), fileWriter,
                keyTypeDescriptor));
    }

    @Override
    protected void doCommit(final EntryWriter<K, V> writer) {
        directoryFacade.renameFileAsync(getTempFileName(), fileName)
                .toCompletableFuture().join();
    }

    private String getTempFileName() {
        return fileName + TEMP_FILE_SUFFIX;
    }
}
