package org.hestiastore.index.unsorteddatafile;

import org.hestiastore.index.PairIterator;
import org.hestiastore.index.PairIteratorStreamer;
import org.hestiastore.index.PairWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeReader;
import org.hestiastore.index.datatype.TypeWriter;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.Directory.Access;
import org.hestiastore.index.directory.FileReader;

/**
 * Unsorted key value pairs storage file.
 * 
 * @author honza
 *
 * @param <K> key type
 * @param <V> value type
 */
public class UnsortedDataFile<K, V> {

    private final Directory directory;

    private final String fileName;

    private final TypeWriter<K> keyWriter;

    private final TypeWriter<V> valueWriter;

    private final TypeReader<K> keyReader;

    private final TypeReader<V> valueReader;

    private final int diskIoBufferSize;

    public static <M, N> UnsortedDataFileBuilder<M, N> builder() {
        return new UnsortedDataFileBuilder<M, N>();
    }

    public UnsortedDataFile(final Directory directory, final String fileName,
            final TypeWriter<K> keyWriter, final TypeWriter<V> valueWriter,
            final TypeReader<K> keyReader, final TypeReader<V> valueReader,
            final int diskIoBufferSize) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.fileName = Vldtn.requireNonNull(fileName, "fileName");
        this.keyWriter = Vldtn.requireNonNull(keyWriter, "keyWriter");
        this.valueWriter = Vldtn.requireNonNull(valueWriter, "");
        this.keyReader = Vldtn.requireNonNull(keyReader, "");
        this.valueReader = Vldtn.requireNonNull(valueReader, "valueReader");
        this.diskIoBufferSize = diskIoBufferSize;
    }

    /**
     * Opens an iterator for this file. If the file does not exist, an empty
     * iterator is returned.
     * 
     * @return opened iterator
     */
    public PairIterator<K, V> openIterator() {
        return new DataFileIterator<>(keyReader, valueReader, getFileReader());
    }

    @Deprecated
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

    /**
     * Opens a transaction writer for this file. If the file does not exist, it
     * is created.
     * 
     * @return a UnsortedDataFileWriterTx instance
     */
    public UnsortedDataFileWriterTx<K, V> openWriterTx() {
        return new UnsortedDataFileWriterTx<>(fileName, directory,
                diskIoBufferSize, keyWriter, valueWriter);
    }

    /**
     * Opens a streamer for this file. If the file does not exist, an empty
     * streamer is returned.
     * 
     * @return opened streamer
     */
    public PairIteratorStreamer<K, V> openStreamer() {
        if (directory.isFileExists(fileName)) {
            return new PairIteratorStreamer<>(openIterator());
        } else {
            return new PairIteratorStreamer<>(null);
        }
    }

    private FileReader getFileReader() {
        return directory.getFileReader(fileName, diskIoBufferSize);
    }

}
