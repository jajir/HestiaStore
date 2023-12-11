package com.coroptis.index.basic;

import java.util.Comparator;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.coroptis.index.IndexException;
import com.coroptis.index.Pair;
import com.coroptis.index.ValueMerger;
import com.coroptis.index.datatype.ConvertorFromBytes;
import com.coroptis.index.datatype.ConvertorToBytes;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeReader;
import com.coroptis.index.datatype.TypeWriter;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.partiallysorteddatafile.PartiallySortedDataFile;
import com.coroptis.index.sstfile.SstFile;
import com.coroptis.index.unsorteddatafile.UnsortedDataFile;

/**
 * 
 * Represents directory with particular index files. Allows to create data files
 * in directory and support further work with them. It support work with
 * individual files.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class BasicIndex<K, V> {

    private final Directory directory;
    private final ValueMerger<K, V> valueMerger;
    private TypeDescriptor<K> keyTypeDescriptor;
    private TypeDescriptor<V> valueTypeDescriptor;

    public BasicIndex(final Directory directory,
            TypeDescriptor<K> keyTypeDescriptor,
            TypeDescriptor<V> valueTypeDescriptor) {
        this(directory, new DefaultValueMerger<>(), keyTypeDescriptor,
                valueTypeDescriptor);
    }

    public BasicIndex(final Directory directory,
            final ValueMerger<K, V> valueMerger,
            TypeDescriptor<K> keyTypeDescriptor,
            TypeDescriptor<V> valueTypeDescriptor) {
        this.directory = Objects.requireNonNull(directory);
        this.valueMerger = Objects.requireNonNull(valueMerger);
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
    }

    public static <M, N> BasicIndexBuilder<M, N> builder() {
        return new BasicIndexBuilder<M, N>();
    }

    protected Directory getDirectory() {
        return directory;
    }

    protected TypeReader<K> getKeyReader() {
        return keyTypeDescriptor.getTypeReader();
    }

    protected TypeReader<V> getValueReader() {
        return valueTypeDescriptor.getTypeReader();
    }

    protected TypeWriter<K> getKeyWriter() {
        return keyTypeDescriptor.getTypeWriter();
    }

    protected TypeWriter<V> getValueWriter() {
        return valueTypeDescriptor.getTypeWriter();
    }

    protected Comparator<K> getKeyComparator() {
        return keyTypeDescriptor.getComparator();
    };

    protected ConvertorFromBytes<K> getKeyConvertorFromBytes() {
        return keyTypeDescriptor.getConvertorFromBytes();
    };

    protected ConvertorToBytes<K> getKeyConvertorToBytes() {
        return keyTypeDescriptor.getConvertorToBytes();
    };

    public UnsortedDataFile<K, V> getUnsortedFile(final String fileName) {
        final UnsortedDataFile<K, V> out = UnsortedDataFile.<K, V>builder()
                .withDirectory(getDirectory()).withFileName(fileName)
                .withKeyReader(getKeyReader()).withValueReader(getValueReader())
                .withKeyWriter(getKeyWriter()).withValueWriter(getValueWriter())
                .build();
        return out;
    }

    public SstFile<K, V> getSortedDataFile(final String fileName) {
        final SstFile<K, V> out = SstFile.<K, V>builder()
                .withDirectory(getDirectory()).withFileName(fileName)
                .withKeyConvertorFromBytes(getKeyConvertorFromBytes())
                .withKeyComparator(getKeyComparator())
                .withKeyConvertorToBytes(getKeyConvertorToBytes())
                .withValueReader(getValueReader())
                .withValueWriter(getValueWriter()).build();
        return out;
    }

    public PartiallySortedDataFile<K, V> getPartiallySortedDataFile(
            final String fileName) {
        final PartiallySortedDataFile<K, V> out = PartiallySortedDataFile
                .<K, V>builder().withFileName(fileName)
                .withKeyComparator(getKeyComparator()).withBasicIndex(this)
                .withValueMerger(valueMerger).build();
        return out;
    }

    /**
     * When it's called sorting of given file starts. For each record in correct
     * order will be called given consumer. When method call returns than there
     * are no more records to sort.
     * 
     * @param unsortedFileName required unsorted file name
     * @param consumer         required consumer. All sorted data will be passed
     *                         to consumer.
     */
    public void consumeSortedData(final String unsortedFileName,
            final Consumer<Pair<K, V>> consumer,
            final Integer howManySortInMemory) {
        final UnsortedDataFileSorter<K, V> sorter = new UnsortedDataFileSorter<>(
                unsortedFileName, valueMerger, getKeyComparator(),
                howManySortInMemory, this);
        sorter.consumeSortedData(consumer);
    }

    public void consumeSortedDataFromPartialySortedDataFile(
            final String partialySortedFileName,
            final Consumer<Pair<K, V>> consumer,
            final Integer howManySortInMemory) {
        final UnsortedDataFileSorter<K, V> sorter = new UnsortedDataFileSorter<>(
                partialySortedFileName, valueMerger, getKeyComparator(),
                howManySortInMemory, this);
        sorter.consumePreSortedData(consumer);
    }

    public boolean deleteFile(final String fileName) {
        if (!directory.deleteFile(fileName)) {
            throw new IndexException(
                    String.format("Unable to delte file '%s' in directory %s",
                            fileName, directory.toString()));
        }
        return directory.deleteFile(fileName);
    }

    public Stream<String> getFileNames() {
        return directory.getFileNames();
    }

}
