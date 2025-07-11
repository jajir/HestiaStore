package org.hestiastore.index.unsorteddatafile;

import org.hestiastore.index.CloseablePairReader;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.PairIteratorFromReader;
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

    public PairIterator<K, V> openIterator() {
        return new PairIteratorFromReader<>(openReader());
    }

    public PairWriter<K, V> openWriter() {
        return openWriter(Access.OVERWRITE);
    }

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

    public UnsortedDataFileStreamer<K, V> openStreamer() {
        if (directory.isFileExists(fileName)) {
            final UnsortedDataFileSpliterator<K, V> spliterator = new UnsortedDataFileSpliterator<>(
                    openReader());
            return new UnsortedDataFileStreamer<>(spliterator);
        } else {
            return new UnsortedDataFileStreamer<>(null);
        }
    }

    public CloseablePairReader<K, V> openReader() {
        return new UnsortedDataFileReader<>(keyReader, valueReader,
                getFileReader());
    }

    private FileReader getFileReader() {
        return directory.getFileReader(fileName, diskIoBufferSize);
    }

}
