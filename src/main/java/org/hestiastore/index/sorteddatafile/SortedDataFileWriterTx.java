package org.hestiastore.index.sorteddatafile;

import org.hestiastore.index.PairWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileWriter;

public class SortedDataFileWriterTx<K, V> implements WriteTransaction<K, V> {

    private static final String TEMP_FILE_SUFFIX = ".tmp";
    private final String fileName;
    private final Directory directory;
    private final int diskIoBufferSize;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;

    /**
     * Constructs a new SortedDataFileWriterTx.
     *
     * @param fileName            required target file name
     * @param directory           required directory to write to
     * @param diskIoBufferSize    the size of the disk I/O buffer
     * @param keyTypeDescriptor   required key type descriptor
     * @param valueTypeDescriptor required value type descriptor
     */
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

    /**
     * Opens a SortedDataFileWriter to write sorted key-value pairs to the
     * temporary file.
     *
     * @return a SortedDataFileWriter instance
     */
    @Override
    public PairWriter<K, V> openWriter() {
        final FileWriter fileWriter = directory.getFileWriter(fileName,
                Directory.Access.OVERWRITE, diskIoBufferSize);
        return new SortedDataFileWriter<>(valueTypeDescriptor.getTypeWriter(),
                fileWriter, keyTypeDescriptor);
    }

    @Override
    public void commit() {
        directory.renameFile(getTempFileName(), fileName);
    }

    private String getTempFileName() {
        return fileName + TEMP_FILE_SUFFIX;
    }

}
