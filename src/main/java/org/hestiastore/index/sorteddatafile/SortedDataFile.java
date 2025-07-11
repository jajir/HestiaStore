package org.hestiastore.index.sorteddatafile;

import org.hestiastore.index.CloseablePairReader;
import org.hestiastore.index.PairIteratorFromReader;
import org.hestiastore.index.PairIteratorWithCurrent;
import org.hestiastore.index.PairReaderEmpty;
import org.hestiastore.index.PairSeekableReader;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileWriter;

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
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.fileName = Vldtn.requireNonNull(fileName, "fileName");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.diskIoBufferSize = Vldtn.ioBufferSize(diskIoBufferSize);
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
        return new PairIteratorFromReader<>(openReader());
    }

    public SortedDataFileWriter<K, V> openWriter() {
        final FileWriter fileWriter = directory.getFileWriter(fileName,
                Directory.Access.OVERWRITE, diskIoBufferSize);
        return new SortedDataFileWriter<>(valueTypeDescriptor.getTypeWriter(),
                fileWriter, keyTypeDescriptor);
    }

    public void delete() {
        directory.deleteFile(fileName);
    }

}
