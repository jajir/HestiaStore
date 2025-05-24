package com.hestiastore.index.sorteddatafile;

import java.util.Objects;

import com.hestiastore.index.CloseablePairReader;
import com.hestiastore.index.PairIteratorFromReader;
import com.hestiastore.index.PairIteratorWithCurrent;
import com.hestiastore.index.PairReaderEmpty;
import com.hestiastore.index.PairSeekableReader;
import com.hestiastore.index.datatype.TypeDescriptor;
import com.hestiastore.index.directory.Directory;
import com.hestiastore.index.directory.FileWriter;

public class SortedDataFile<K, V> {

    private final Directory directory;

    private final String fileName;

    private final int diskIoBufferSize;

    private final TypeDescriptor<K> keyTypeDescriptor;

    private final TypeDescriptor<V> valueTypeDescriptor;

    public static <M, N> SortedDataFileBuilder<M, N> builder() {
        return new SortedDataFileBuilder<M, N>();
    }

    public SortedDataFile(final Directory directory, final String fileName,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final int diskIoBufferSize) {
        this.directory = Objects.requireNonNull(directory);
        this.fileName = Objects.requireNonNull(fileName);
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
        this.diskIoBufferSize = diskIoBufferSize;
    }

    public SortedDataFile<K, V> withFileName(final String newFileName) {
        return new SortedDataFile<>(directory, newFileName, keyTypeDescriptor,
                valueTypeDescriptor, diskIoBufferSize);
    }

    public SortedDataFile<K, V> withProperties(final Directory newDirectory,
            final String newFileName, final int newDiskIoBufferSize) {
        return new SortedDataFile<>(newDirectory, newFileName,
                keyTypeDescriptor, valueTypeDescriptor, newDiskIoBufferSize);
    }

    public CloseablePairReader<K, V> openReader() {
        return openReader(0);
    }

    public CloseablePairReader<K, V> openReader(final long position) {
        if (!directory.isFileExists(fileName)) {
            return new PairReaderEmpty<>();
        }
        final DiffKeyReader<K> diffKeyReader = new DiffKeyReader<>(
                keyTypeDescriptor.getConvertorFromBytes());
        final SortedDataFileReader<K, V> reader = new SortedDataFileReader<>(
                diffKeyReader, valueTypeDescriptor.getTypeReader(),
                directory.getFileReader(fileName, diskIoBufferSize));
        reader.skip(position);
        return reader;
    }

    public PairSeekableReader<K, V> openSeekableReader() {
        if (!directory.isFileExists(fileName)) {
            return new PairReaderEmpty<>();
        }
        final DiffKeyReader<K> diffKeyReader = new DiffKeyReader<>(
                keyTypeDescriptor.getConvertorFromBytes());
        return new PairSeekableReaderImpl<>(diffKeyReader,
                valueTypeDescriptor.getTypeReader(),
                directory.getFileReaderSeekable(fileName));
    }

    public PairIteratorWithCurrent<K, V> openIterator() {
        final PairIteratorWithCurrent<K, V> iterator = new PairIteratorFromReader<>(
                openReader());
        return iterator;
    }

    public SortedDataFileWriter<K, V> openWriter() {
        final FileWriter fileWriter = directory.getFileWriter(fileName,
                Directory.Access.OVERWRITE, diskIoBufferSize);
        final SortedDataFileWriter<K, V> writer = new SortedDataFileWriter<>(
                valueTypeDescriptor.getTypeWriter(), fileWriter,
                keyTypeDescriptor);
        return writer;
    }

    public void delete() {
        directory.deleteFile(fileName);
    }

}
