package org.hestiastore.index.unsorteddatafile;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryIteratorStreamer;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeReader;
import org.hestiastore.index.datatype.TypeWriter;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileReader;

final class UnsortedDataFileImpl<K, V> implements UnsortedDataFile<K, V> {

    private final Directory directory;
    private final String fileName;
    private final TypeWriter<K> keyWriter;
    private final TypeWriter<V> valueWriter;
    private final TypeReader<K> keyReader;
    private final TypeReader<V> valueReader;
    private final int diskIoBufferSize;

    UnsortedDataFileImpl(final Directory directory, final String fileName,
            final TypeWriter<K> keyWriter, final TypeWriter<V> valueWriter,
            final TypeReader<K> keyReader, final TypeReader<V> valueReader,
            final int diskIoBufferSize) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.fileName = Vldtn.requireNonNull(fileName, "fileName");
        this.keyWriter = Vldtn.requireNonNull(keyWriter, "keyWriter");
        this.valueWriter = Vldtn.requireNonNull(valueWriter, "valueWriter");
        this.keyReader = Vldtn.requireNonNull(keyReader, "keyReader");
        this.valueReader = Vldtn.requireNonNull(valueReader, "valueReader");
        this.diskIoBufferSize = diskIoBufferSize;
    }

    /**
     * Opens a fresh iterator over the current file contents. The caller must
     * close the iterator when finished.
     */
    @Override
    public EntryIterator<K, V> openIterator() {
        return new DataFileIterator<>(keyReader, valueReader, getFileReader());
    }

    /**
     * Creates a transactional writer that writes to a temporary file and
     * promotes it upon commit.
     */
    @Override
    public UnsortedDataFileWriterTx<K, V> openWriterTx() {
        return new UnsortedDataFileWriterTx<>(fileName, directory,
                diskIoBufferSize, keyWriter, valueWriter);
    }

    /**
     * Provides a streaming view over the file contents. When the file does not
     * exist the returned stream is empty.
     */
    @Override
    public EntryIteratorStreamer<K, V> openStreamer() {
        if (directory.isFileExists(fileName)) {
            return new EntryIteratorStreamer<>(openIterator());
        } else {
            return new EntryIteratorStreamer<>(null);
        }
    }

    private FileReader getFileReader() {
        return directory.getFileReader(fileName, diskIoBufferSize);
    }
}
