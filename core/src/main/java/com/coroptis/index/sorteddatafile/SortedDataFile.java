package com.coroptis.index.sorteddatafile;

import java.util.Comparator;
import java.util.Objects;

import com.coroptis.index.CloseablePairReader;
import com.coroptis.index.PairIteratorFromReader;
import com.coroptis.index.PairIteratorWithCurrent;
import com.coroptis.index.PairReaderEmpty;
import com.coroptis.index.PairSeekableReader;
import com.coroptis.index.datatype.ConvertorFromBytes;
import com.coroptis.index.datatype.ConvertorToBytes;
import com.coroptis.index.datatype.TypeReader;
import com.coroptis.index.datatype.TypeWriter;
import com.coroptis.index.directory.Directory;

public class SortedDataFile<K, V> {

    private final Directory directory;

    private final String fileName;

    private final int diskIoBufferSize;

    private final TypeWriter<V> valueWriter;

    private final TypeReader<V> valueReader;

    private final Comparator<K> keyComparator;

    private final ConvertorFromBytes<K> keyConvertorFromBytes;

    private final ConvertorToBytes<K> keyConvertorToBytes;

    public static <M, N> SortedDataFileBuilder<M, N> builder() {
        return new SortedDataFileBuilder<M, N>();
    }

    public SortedDataFile(final Directory directory, final String fileName,
            final TypeWriter<V> valueWriter, final TypeReader<V> valueReader,
            final Comparator<K> keyComparator,
            final ConvertorFromBytes<K> keyConvertorFromBytes,
            final ConvertorToBytes<K> keyConvertorToBytes,
            final int diskIoBufferSize) {
        this.directory = Objects.requireNonNull(directory);
        this.fileName = Objects.requireNonNull(fileName);
        this.valueWriter = Objects.requireNonNull(valueWriter);
        this.valueReader = Objects.requireNonNull(valueReader);
        this.keyComparator = Objects.requireNonNull(keyComparator);
        this.keyConvertorFromBytes = Objects
                .requireNonNull(keyConvertorFromBytes);
        this.keyConvertorToBytes = Objects.requireNonNull(keyConvertorToBytes);
        this.diskIoBufferSize = diskIoBufferSize;
    }

    public SortedDataFile<K, V> withFileName(final String newFileName) {
        return new SortedDataFile<>(directory, newFileName, valueWriter, valueReader,
                keyComparator, keyConvertorFromBytes, keyConvertorToBytes,
                diskIoBufferSize);
    }

    public SortedDataFile<K, V> withProperties(final Directory newDirectory, final String newFileName, final int newDiskIoBufferSize) {
        return new SortedDataFile<>(newDirectory, newFileName, valueWriter, valueReader, keyComparator, keyConvertorFromBytes, keyConvertorToBytes, newDiskIoBufferSize);
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
        final SortedDataFileReader<K, V> reader = new SortedDataFileReader<>(diffKeyReader,
                valueReader,
                directory.getFileReader(fileName, diskIoBufferSize));
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

    public PairIteratorWithCurrent<K, V> openIterator() {
        final PairIteratorWithCurrent<K, V> iterator = new PairIteratorFromReader<>(
                openReader());
        return iterator;
    }

    public SortedDataFileWriter<K, V> openWriter() {
        final SortedDataFileWriter<K, V> writer = new SortedDataFileWriter<>(directory,
                fileName, keyConvertorToBytes, keyComparator, valueWriter,
                diskIoBufferSize);
        return writer;
    }

    public void delete() {
        directory.deleteFile(fileName);
    }

}
