package com.coroptis.index.sorteddatafile;

import java.util.Objects;

import com.coroptis.index.CloseablePairReader;
import com.coroptis.index.PairIteratorFromReader;
import com.coroptis.index.PairIteratorWithCurrent;
import com.coroptis.index.PairReaderEmpty;
import com.coroptis.index.PairSeekableReader;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.FileWriter;

public class SortedDataFile<K, V> {

    private final Directory directory;

    private final String fileName;

    private final int diskIoBufferSize;;

    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;

    private final DataCompressor dataCompressor;

    public static <M, N> SortedDataFileBuilder<M, N> builder() {
        return new SortedDataFileBuilder<M, N>();
    }

    public SortedDataFile(final Directory directory, final String fileName,
            final int diskIoBufferSize,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final DataCompressor dataCompressor) {
        this.directory = Objects.requireNonNull(directory);
        this.fileName = Objects.requireNonNull(fileName);
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
        this.diskIoBufferSize = diskIoBufferSize;
        this.dataCompressor = Objects.requireNonNull(dataCompressor);
    }

    public SortedDataFile<K, V> withFileName(final String newFileName) {
        return new SortedDataFile<>(directory, newFileName, diskIoBufferSize, keyTypeDescriptor, valueTypeDescriptor,
                dataCompressor);
    }

    public SortedDataFile<K, V> withProperties(final Directory newDirectory, final String newFileName,
            final int newDiskIoBufferSize) {
        return new SortedDataFile<>(newDirectory, newFileName, newDiskIoBufferSize, keyTypeDescriptor,
                valueTypeDescriptor, dataCompressor);
    }

    public CloseablePairReader<K, V> openReader() {
        return openReader(0);
    }

    public CloseablePairReader<K, V> openReader(final long position) {
        if (!directory.isFileExists(fileName)) {
            return new PairReaderEmpty<>();
        }
        final DiffKeyReader<K> diffKeyReader = new DiffKeyReader<K>(
                keyTypeDescriptor.getConvertorFromBytes());
        final SortedDataFileReader<K, V> reader = new SortedDataFileReader<>(diffKeyReader,
                valueTypeDescriptor.getTypeReader(),
                directory.getFileReader(fileName, diskIoBufferSize),null);
        if (position > 0) {
            reader.skip(position);
        }
        return reader;
    }

    public PairSeekableReader<K, V> openSeekableReader() {
        if (!directory.isFileExists(fileName)) {
            return new PairReaderEmpty<>();
        }
        final DiffKeyReader<K> diffKeyReader = new DiffKeyReader<K>(
                keyTypeDescriptor.getConvertorFromBytes());
        return new PairSeekableReaderImpl<>(diffKeyReader, valueTypeDescriptor.getTypeReader(),
                directory.getFileReaderSeekable(fileName));
    }

    public PairIteratorWithCurrent<K, V> openIterator() {
        final PairIteratorWithCurrent<K, V> iterator = new PairIteratorFromReader<>(
                openReader());
        return iterator;
    }

    public SortedDataFileWriter<K, V> openWriter() {
        final FileWriter writer = directory.getFileWriter(fileName,
                Directory.Access.OVERWRITE, diskIoBufferSize);
        final CompressingWriter compressingWriter = new CompressingWriter(writer, dataCompressor);
        final SeekeableFileWriter seekeableFileWriter = new SeekeableFileWriterImpl(compressingWriter);
        final SortedDataFileWriter<K, V> sortedDataFileWriter = new SortedDataFileWriter<>(seekeableFileWriter,
                keyTypeDescriptor.getConvertorToBytes(), keyTypeDescriptor.getComparator(),
                valueTypeDescriptor.getConvertorToBytes());
        return sortedDataFileWriter;
    }

    public void delete() {
        directory.deleteFile(fileName);
    }

}
