package com.coroptis.index.basic;

import java.util.Comparator;
import java.util.Objects;
import java.util.function.Consumer;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.ValueMerger;
import com.coroptis.index.partiallysorteddatafile.PartiallySortedDataFileWriter;
import com.coroptis.index.unsorteddatafile.UnsortedDataFile;

/**
 * 1. merge two indexes into one.
 * 
 * 
 * 2. split unsorted index into n.
 * 
 * 
 * @param <K>
 * @param <V>
 */
class UnsortedDataFileSorter<K, V> {

    private final static int HOW_MANY_FILES_TO_MERGE_AT_ONCE = 50;

    /**
     * Unsorted data file name.
     */
    private final String unsortedFileName;
    private final ValueMerger<K, V> merger;
    private final Comparator<K> keyComparator;
    private final Integer howManySortInMemory;
    private final BasicIndex<K, V> basicIndex;
    private final SortSupport<K, V> sortSupport;
    private final RoundSorted<K, V> roundSorter;

    UnsortedDataFileSorter(final String unsortedFileName,
            final ValueMerger<K, V> merger, final Comparator<K> keyComparator,
            final Integer howManySortInMemory,
            final BasicIndex<K, V> basicIndex) {
        this.unsortedFileName = Objects.requireNonNull(unsortedFileName);
        this.merger = Objects.requireNonNull(merger);
        this.howManySortInMemory = Objects.requireNonNull(howManySortInMemory);
        this.basicIndex = Objects.requireNonNull(basicIndex);
        this.keyComparator = Objects.requireNonNull(keyComparator);
        this.sortSupport = new SortSupport<>(basicIndex, merger,
                unsortedFileName);
        this.roundSorter = new RoundSorted<>(basicIndex, sortSupport,
                HOW_MANY_FILES_TO_MERGE_AT_ONCE);
    }

    void consumeSortedData(final Consumer<Pair<K, V>> consumer) {
        splitIntoSortedIndexes();
        consumePreSortedData(consumer);
    }

    void consumePreSortedData(final Consumer<Pair<K, V>> consumer) {
        Objects.requireNonNull(consumer);
        int roundNo = 0;
        while (roundSorter.mergeRound(roundNo, consumer)) {
            roundNo++;
        }
    }

    private void splitIntoSortedIndexes() {
        try (final PartiallySortedDataFileWriter<K, V> writer = new PartiallySortedDataFileWriter<>(
                unsortedFileName, merger, howManySortInMemory, basicIndex,
                keyComparator)) {
            final UnsortedDataFile<K, V> unsortedFile = basicIndex
                    .getUnsortedFile(unsortedFileName);
            try (final PairIterator<K, V> reader = unsortedFile
                    .openIterator()) {
                while (reader.hasNext()) {
                    final Pair<K, V> pair = reader.readCurrent().get();
                    writer.put(pair);
                }
            }
            basicIndex.deleteFile(unsortedFileName);
        }
    }

}
