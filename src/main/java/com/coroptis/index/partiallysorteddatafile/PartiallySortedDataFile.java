package com.coroptis.index.partiallysorteddatafile;

import java.util.Comparator;
import java.util.Objects;

import com.coroptis.index.ValueMerger;
import com.coroptis.index.basic.BasicIndex;
import com.coroptis.index.basic.SortSupport;

public class PartiallySortedDataFile<K, V> {

    private final String fileName;

    private final Comparator<K> keyComparator;

    private final BasicIndex<K, V> basicIndex;

    private final ValueMerger<K, V> merger;

    public static <M, N> PartiallySortedDataFileBuilder<M, N> builder() {
        return new PartiallySortedDataFileBuilder<M, N>();
    }

    public PartiallySortedDataFile(final String fileName,
            final Comparator<K> keyComparator,
            final BasicIndex<K, V> basicIndex, final ValueMerger<K, V> merger) {
        this.fileName = Objects.requireNonNull(fileName);
        this.keyComparator = Objects.requireNonNull(keyComparator);
        this.basicIndex = Objects.requireNonNull(basicIndex);
        this.merger = Objects.requireNonNull(merger);
    }

    public PartiallySortedDataFileReader<K, V> openReader() {
        final SortSupport<K, V> sortSupport = new SortSupport<>(basicIndex,
                merger, fileName);
        final PartiallySortedDataFileReader<K, V> reader = new PartiallySortedDataFileReader<>(
                basicIndex, sortSupport);
        return reader;
    }

    public PartiallySortedDataFileWriter<K, V> openWriter(
            int howManySortInMemory) {
        final PartiallySortedDataFileWriter<K, V> writer = new PartiallySortedDataFileWriter<>(
                fileName, merger, howManySortInMemory, basicIndex,
                keyComparator);
        return writer;
    }

}
