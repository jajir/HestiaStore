package org.hestiastore.index.sorteddatafile;

import org.hestiastore.index.GuardedPairWriter;
import org.hestiastore.index.GuardedWriteTransaction;
import org.hestiastore.index.PairWriter;
import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileWriter;

public class SortedDataFileWriterTx<K, V>
        extends GuardedWriteTransaction<PairWriter<K, V>>
        implements WriteTransaction<K, V> {

    private static final String TEMP_FILE_SUFFIX = ".tmp";
    private final String fileName;
    private final Directory directory;
    private final int diskIoBufferSize;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;

    public SortedDataFileWriterTx(final String fileName,
            final Directory directory, final int diskIoBufferSize,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.fileName = Vldtn.requireNonNull(fileName, "fileName");
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.diskIoBufferSize = diskIoBufferSize;
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
    }

    @Override
    protected PairWriter<K, V> doOpen() {
        final FileWriter fileWriter = directory.getFileWriter(
                getTempFileName(), Directory.Access.OVERWRITE,
                diskIoBufferSize);
        return new GuardedPairWriter<>(new SortedDataFileWriter<>(
                valueTypeDescriptor.getTypeWriter(), fileWriter,
                keyTypeDescriptor));
    }

    @Override
    protected void doCommit(final PairWriter<K, V> writer) {
        directory.renameFile(getTempFileName(), fileName);
    }

    private String getTempFileName() {
        return fileName + TEMP_FILE_SUFFIX;
    }
}
