package org.hestiastore.index.sorteddatafile;

import java.util.ArrayList;
import java.util.List;

import org.hestiastore.index.FileNameUtil;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryIteratorWithCurrent;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.cache.UniqueCache;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.unsorteddatafile.UnsortedDataFile;

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

    private static final int COUNT_MAX_LENGTH = 5;
    private static final String MERGING_FILES_PREFIX = "merging-";
    private static final String MERGING_FILES_SUFFIX = ".tmp";
    private static final int ROUND_ZERO = 0;
    private static final int MERGING_FILE_CAP = 10;

    private final UnsortedDataFile<K, V> unsortedDataFile;
    private final SortedDataFile<K, V> targetSortedDataFile;
    private final Merger<K, V> merger;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final int maxNumberOfKeysInMemory;

    public DataFileSorter(final UnsortedDataFile<K, V> unsortedDataFile,
            final SortedDataFile<K, V> sortedDataFile,
            final Merger<K, V> merger,
            final TypeDescriptor<K> keyTypeDescriptor,
            final int maxNumberOfKeysInMemory) {
        this.unsortedDataFile = Vldtn.requireNonNull(unsortedDataFile,
                "unsortedDataFile");
        this.targetSortedDataFile = Vldtn.requireNonNull(sortedDataFile,
                "sortedDataFile");
        this.merger = Vldtn.requireNonNull(merger, "merger");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.maxNumberOfKeysInMemory = maxNumberOfKeysInMemory;
    }

    public void sort() {
        /*
         * 1. Split data to chunks, record number of chunks 2. run merging
         * round, if then l
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
                keyTypeDescriptor.getComparator(), maxNumberOfKeysInMemory);
        int chunkCount = 0;
        try (EntryIterator<K, V> iterator = unsortedDataFile.openIterator()) {
            while (iterator.hasNext()) {
                final Entry<K, V> entry = iterator.next();
                cache.put(entry);
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
        chunkFile.openWriterTx().execute(writer -> {
            cache.getAsSortedList().forEach(entry -> writer.write(entry));
        });
    }

    private SortedDataFile<K, V> getChunkFile(final int round,
            final int chunkCount) {
        final String prefix = MERGING_FILES_PREFIX
                + FileNameUtil.getPaddedId(round, 3) + "-";
        final String fileName = FileNameUtil.getFileName(prefix, chunkCount,
                COUNT_MAX_LENGTH, MERGING_FILES_SUFFIX);
        return targetSortedDataFile.withFileName(fileName);
    }

    private int mergeChunks(final int round, final int chunkCount) {
        if (chunkCount < MERGING_FILE_CAP) {
            // last round
            mergeIndexFiles(round, 0, chunkCount, targetSortedDataFile);
            return 1;
        } else {
            int fileCount = 0;
            int index = 0;
            while (index < chunkCount) {
                mergeIndexFiles(round, index, index + MERGING_FILE_CAP,
                        getChunkFile(round + 1, fileCount));
                index += MERGING_FILE_CAP;
                fileCount++;
            }
            return fileCount;
        }

    }

    private void mergeIndexFiles(final int round, final int fromFileIndex,
            final int toFileIndex, SortedDataFile<K, V> targetFile) {
        final List<EntryIteratorWithCurrent<K, V>> chunkFiles = new ArrayList<>(
                toFileIndex - fromFileIndex);
        for (int i = fromFileIndex; i < toFileIndex; i++) {
            chunkFiles.add(getChunkFile(round, i).openIterator());
        }
        mergeFiles(chunkFiles, targetFile);
        for (int i = fromFileIndex; i < toFileIndex; i++) {
            getChunkFile(round, i).delete();
        }
    }

    private void mergeFiles(
            final List<EntryIteratorWithCurrent<K, V>> chunkFiles,
            SortedDataFile<K, V> targetFile) {
        try (MergedEntryIterator<K, V> iterator = new MergedEntryIterator<>(
                chunkFiles, keyTypeDescriptor.getComparator(), merger)) {
            targetFile.openWriterTx().execute(writer -> {
                Entry<K, V> entry = null;
                while (iterator.hasNext()) {
                    entry = iterator.next();
                    writer.write(entry);
                }
            });
        }
    }

}
