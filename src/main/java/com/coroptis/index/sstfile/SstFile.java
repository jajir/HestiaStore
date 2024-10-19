package com.coroptis.index.sstfile;

import java.util.Comparator;
import java.util.Objects;

import com.coroptis.index.PairIterator;
import com.coroptis.index.PairIteratorFromReader;
import com.coroptis.index.CloseablePairReader;
import com.coroptis.index.PairReaderEmpty;
import com.coroptis.index.PairSeekableReader;
import com.coroptis.index.datatype.ConvertorFromBytes;
import com.coroptis.index.datatype.ConvertorToBytes;
import com.coroptis.index.datatype.TypeReader;
import com.coroptis.index.datatype.TypeWriter;
import com.coroptis.index.directory.Directory;

public class SstFile<K, V> {

    private final Directory directory;

    private final String fileName;

    private final int fileReadingBufferSize;

    private final TypeWriter<V> valueWriter;

    private final TypeReader<V> valueReader;

    private final Comparator<K> keyComparator;

    private final ConvertorFromBytes<K> keyConvertorFromBytes;

    private final ConvertorToBytes<K> keyConvertorToBytes;

    public static <M, N> SstFileBuilder<M, N> builder() {
        return new SstFileBuilder<M, N>();
    }

    public SstFile(final Directory directory, final String fileName,
            final TypeWriter<V> valueWriter, final TypeReader<V> valueReader,
            final Comparator<K> keyComparator,
            final ConvertorFromBytes<K> keyConvertorFromBytes,
            final ConvertorToBytes<K> keyConvertorToBytes,
            final int fileReadingBufferSize) {
        this.directory = Objects.requireNonNull(directory);
        this.fileName = Objects.requireNonNull(fileName);
        this.valueWriter = Objects.requireNonNull(valueWriter);
        this.valueReader = Objects.requireNonNull(valueReader);
        this.keyComparator = Objects.requireNonNull(keyComparator);
        this.keyConvertorFromBytes = Objects
                .requireNonNull(keyConvertorFromBytes);
        this.keyConvertorToBytes = Objects.requireNonNull(keyConvertorToBytes);
        this.fileReadingBufferSize = fileReadingBufferSize;
    }

    public CloseablePairReader<K, V> openReader() {
        return openReader(0);
    }

    public CloseablePairReader<K, V> openReader(final long position) {
        if (!directory.isFileExists(fileName)) {
            return new PairReaderEmpty<>();
        }
        final DiffKeyReader<K> diffKeyReader = new DiffKeyReader<K>(
                keyConvertorFromBytes);
        final SstFileReader<K, V> reader = new SstFileReader<>(diffKeyReader,
                valueReader,
                directory.getFileReader(fileName, fileReadingBufferSize));
        reader.skip(position);
        return reader;
    }

    public PairSeekableReader<K, V> openSeekableReader() {
        if (!directory.isFileExists(fileName)) {
            return new PairReaderEmpty<>();
        }
        final DiffKeyReader<K> diffKeyReader = new DiffKeyReader<K>(
                keyConvertorFromBytes);
        return new PairSeekableReaderImpl<>(diffKeyReader, valueReader,
                directory.getFileReaderSeekable(fileName));
    }

    @SuppressWarnings("resource")
    public PairIterator<K, V> openIterator() {
        final PairIterator<K, V> iterator = new PairIteratorFromReader<>(
                openReader());
        return iterator;
    }

    public SstFileWriter<K, V> openWriter() {
        final SstFileWriter<K, V> writer = new SstFileWriter<>(directory,
                fileName, keyConvertorToBytes, keyComparator, valueWriter);
        return writer;
    }

}
