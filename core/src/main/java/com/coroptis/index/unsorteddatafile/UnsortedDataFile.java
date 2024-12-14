package com.coroptis.index.unsorteddatafile;

import java.util.Objects;

import com.coroptis.index.CloseablePairReader;
import com.coroptis.index.PairIterator;
import com.coroptis.index.PairIteratorFromReader;
import com.coroptis.index.PairWriter;
import com.coroptis.index.datatype.TypeReader;
import com.coroptis.index.datatype.TypeWriter;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.Directory.Access;
import com.coroptis.index.directory.FileReader;

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

    public static <M, N> UnsortedDataFileBuilder<M, N> builder() {
        return new UnsortedDataFileBuilder<M, N>();
    }

    public UnsortedDataFile(final Directory directory, final String fileName,
            final TypeWriter<K> keyWriter, final TypeWriter<V> valueWriter,
            final TypeReader<K> keyReader, final TypeReader<V> valueReader) {
        this.directory = Objects.requireNonNull(directory);
        this.fileName = Objects.requireNonNull(fileName);
        this.keyWriter = Objects.requireNonNull(keyWriter);
        this.valueWriter = Objects.requireNonNull(valueWriter);
        this.keyReader = Objects.requireNonNull(keyReader);
        this.valueReader = Objects.requireNonNull(valueReader);
    }

    public PairIterator<K, V> openIterator() {
        final PairIterator<K, V> iterator = new PairIteratorFromReader<>(
                openReader());
        return iterator;
    }

    public PairIterator<K, V> openIterator(int bufferSize) {
        final PairIterator<K, V> iterator = new PairIteratorFromReader<>(
                openReader(bufferSize));
        return iterator;
    }

    public PairWriter<K, V> openWriter() {
        return openWriter(Access.OVERWRITE);
    }

    public PairWriter<K, V> openWriter(final Access access) {
        Objects.requireNonNull(access);
        Access used = null;
        if (directory.isFileExists(fileName)) {
            used = access;
        } else {
            used = Access.OVERWRITE;
        }
        final UnsortedDataFileWriter<K, V> writer = new UnsortedDataFileWriter<>(
                directory, fileName, keyWriter, valueWriter, used);
        return writer;
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
        final UnsortedDataFileReader<K, V> out = new UnsortedDataFileReader<>(
                keyReader, valueReader, getFileReader());
        return out;
    }

    public CloseablePairReader<K, V> openReader(int bufferSize) {
        final UnsortedDataFileReader<K, V> out = new UnsortedDataFileReader<>(
                keyReader, valueReader, getFileReader(bufferSize));
        return out;
    }

    private FileReader getFileReader() {
        return directory.getFileReader(fileName);
    }

    private FileReader getFileReader(int bufferSize) {
        return directory.getFileReader(fileName, bufferSize);
    }

}
