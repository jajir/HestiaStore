package com.coroptis.index.sorteddatafile;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.coroptis.index.FileNameUtil;
import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.PairIteratorWithCurrent;
import com.coroptis.index.cache.UniqueCache;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.unsorteddatafile.UnsortedDataFile;

/**
 * Class transform unsorted data file to sorted data file.
 * 
 * Sorting is done in a ffollowing way:
 * 
 * <ul>
 * <li>Read chunk of data from input file</li>
 * <li>Sort this data and store them into temp file</li>
 * <li>Repeat until all data are sorted in temp files</li>
 * <li>Merge all temp files into one sorted file</li>
 * </ul>
 */
public class DataFileSorter<K, V> {

    private final static int COUNT_MAX_LENGTH = 5;
    private final static String MERGING_FILES_PREFIX = "merging-";
    private final static String MERGING_FILES_SUFFIX = ".tmp";

    private final int ROUND_ZERO = 0;

    private final int mergingFileCap = 10;
    private final UnsortedDataFile<K, V> unsortedDataFile;
    private final SortedDataFile<K, V> targetSortedDataFile;
    private final Merger<K, V> merger;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final long maxNumberOfKeysInMemory;

    public DataFileSorter(final UnsortedDataFile<K, V> unsortedDataFile,
            final SortedDataFile<K, V> sortedDataFile, final Merger<K, V> merger,
            final TypeDescriptor<K> keyTypeDescriptor,
            final long maxNumberOfKeysInMemory) {
        this.unsortedDataFile = Objects.requireNonNull(unsortedDataFile,
                "unsortedDataFile must not be null");
        this.targetSortedDataFile = Objects.requireNonNull(sortedDataFile,
                "sortedDataFile must not be null");
        this.merger = Objects.requireNonNull(merger, "merger must not be null");
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor must not be null");
        this.maxNumberOfKeysInMemory = maxNumberOfKeysInMemory;
    }

    public void sort() {
        /*
         * 1. Split data to chunks, record number of chunks
         * 2. run merging round, if then l
         * 
         */
        int round = 0;
        int numbeorOfChunks = splitToChunks();

        while (numbeorOfChunks > 1) {
            numbeorOfChunks = mergeChunks(round, numbeorOfChunks);
            round++;
        }
    }

    private int splitToChunks() {
        final UniqueCache<K, V> cache = new UniqueCache<>(
                keyTypeDescriptor.getComparator());
        int chunkCount = 0;
        try (PairIterator<K, V> iterator = unsortedDataFile.openIterator()) {
            while (iterator.hasNext()) {
                final Pair<K, V> pair = iterator.next();
                cache.put(pair);
                if (cache.size() >= maxNumberOfKeysInMemory) {
                    writeChunkToFile(cache, ROUND_ZERO, chunkCount);
                    cache.clear();
                    chunkCount++;
                }
            }

        }

        writeChunkToFile(cache, ROUND_ZERO, chunkCount);
        chunkCount++;
        return chunkCount;
    }

    private void writeChunkToFile(final UniqueCache<K, V> cache,
            final int round, final int chunkCount) {
        final SortedDataFile<K, V> chunkFile = getChunkFile(round, chunkCount);
        try (SortedDataFileWriter<K, V> writer = chunkFile.openWriter()) {
            cache.getAsSortedList().forEach(pair -> writer.write(pair));
        }
    }

    private final SortedDataFile<K, V> getChunkFile(final int round, final int chunkCount) {
        final String prefix = MERGING_FILES_PREFIX + FileNameUtil.getPaddedId(round, 3) + "-";
        final String fileName = FileNameUtil.getFileName(prefix, chunkCount,
                COUNT_MAX_LENGTH, MERGING_FILES_SUFFIX);
        return targetSortedDataFile.withFileName(fileName);
    }

    private int mergeChunks(final int round, final int chunkCount) {
        if (chunkCount < mergingFileCap) {
            // last round
            mergeIndexFiles(round, 0, chunkCount, targetSortedDataFile);
            return 1;
        } else {
            int fileCount = 0;
            int index = 0;
            while (index < chunkCount) {
                mergeIndexFiles(round, index, index + mergingFileCap, getChunkFile(round + 1, fileCount));
                index += mergingFileCap;
                fileCount++;
            }
            return fileCount;
        }

    }

    private void mergeIndexFiles(final int round, final int fromFileIndex, final int toFileIndex,
            SortedDataFile<K, V> targetFile) {
        final List<PairIteratorWithCurrent<K, V>> chunkFiles = new ArrayList<>(
                toFileIndex - fromFileIndex);
        for (int i = fromFileIndex; i < toFileIndex; i++) {
            chunkFiles.add(getChunkFile(round, i).openIterator());
        }
        mergeFiles(chunkFiles, targetFile);
        for (int i = fromFileIndex; i < toFileIndex; i++) {
            getChunkFile(round, i).delete();
        }
    }

    private void mergeFiles(final List<PairIteratorWithCurrent<K, V>> chunkFiles, SortedDataFile<K, V> targetFile) {
        try (MergedPairIterator<K, V> iterator = new MergedPairIterator<>(
                chunkFiles, keyTypeDescriptor.getComparator(), merger)) {
            try (SortedDataFileWriter<K, V> writer = targetFile.openWriter()) {
                Pair<K, V> pair = null;
                while (iterator.hasNext()) {
                    pair = iterator.next();
                    writer.write(pair);
                }
            }
        }
    }

}
