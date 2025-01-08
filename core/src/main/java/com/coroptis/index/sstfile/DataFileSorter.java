package com.coroptis.index.sstfile;

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

    private final UnsortedDataFile<K, V> unsortedDataFile;
    private final SstFile<K, V> sortedDataFile;
    private final Merger<K, V> merger;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final long maxNumberOfKeysInMemory;

    public DataFileSorter(final UnsortedDataFile<K, V> unsortedDataFile,
            final SstFile<K, V> sortedDataFile, final Merger<K, V> merger,
            final TypeDescriptor<K> keyTypeDescriptor,
            final long maxNumberOfKeysInMemory) {
        this.unsortedDataFile = Objects.requireNonNull(unsortedDataFile,
                "unsortedDataFile must not be null");
        this.sortedDataFile = Objects.requireNonNull(sortedDataFile,
                "sortedDataFile must not be null");
        this.merger = Objects.requireNonNull(merger, "merger must not be null");
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor must not be null");
        this.maxNumberOfKeysInMemory = maxNumberOfKeysInMemory;
    }

    public void sort() {
        final int numbeorOfChunks = splitToChunks();
        mergeChunks(numbeorOfChunks);
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
                    writeChunkToFile(cache, chunkCount);
                    cache.clear();
                    chunkCount++;
                }
            }

        }

        if (!cache.isEmpty()) {
            writeChunkToFile(cache, chunkCount);
            chunkCount++;
        }
        return chunkCount;
    }

    private void writeChunkToFile(final UniqueCache<K, V> cache,
            final int chunkCount) {
        final SstFile<K, V> chunkFile = getChunkFile(chunkCount);
        try (SstFileWriter<K, V> writer = chunkFile.openWriter()) {
            cache.getAsSortedList().forEach(pair -> writer.put(pair));
        }
    }

    private final SstFile<K, V> getChunkFile(final int chunkCount) {
        final String fileName = FileNameUtil.getFileName("pokus-", chunkCount,
                COUNT_MAX_LENGTH, ".tmp");
        return sortedDataFile.withFileName(fileName);
    }

    private void mergeChunks(final int chunkCount) {
        final List<PairIteratorWithCurrent<K, V>> chunkFiles = new ArrayList<>(
                chunkCount);
        for (int i = 0; i < chunkCount; i++) {
            chunkFiles.add(getChunkFile(i).openIterator());
        }
        try (MergedPairIterator<K, V> iterator = new MergedPairIterator<>(
                chunkFiles, keyTypeDescriptor.getComparator(), merger)) {
            try (SstFileWriter<K, V> writer = sortedDataFile.openWriter()) {
                Pair<K, V> pair = null;
                while (iterator.hasNext()) {
                    pair =iterator.next();
                    System.out.println(pair);
                    writer.put(pair);
                }
            }
        }
        for (int i = 0; i < chunkCount; i++) {
            getChunkFile(i).delete();
        }
    }

}
