package com.coroptis.index.simpledatafile;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.IndexException;
import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.PairIteratorFromReader;
import com.coroptis.index.CloseablePairReader;
import com.coroptis.index.PairWriter;
import com.coroptis.index.ValueMerger;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.Directory.Access;
import com.coroptis.index.directory.Props;
import com.coroptis.index.sstfile.SstFile;
import com.coroptis.index.sstfile.SstFileWriter;
import com.coroptis.index.unsorteddatafile.UnsortedDataFile;
import com.coroptis.index.unsorteddatafile.UnsortedDataFileWriter;

/**
 * It's not thread safe. Access from multiple threads could easily corrupt
 * index.
 * 
 * 
 * @author honza
 *
 * @param <K> key tyoe
 * @param <V> value type
 */
public class SimpleDataFile<K, V> {

    private final Logger logger = LoggerFactory.getLogger(SimpleDataFile.class);
    final static String NUMBER_OF_KEY_VALUE_PAIRS_IN_MAIN_FILE = "number_of_key_value_pairs_in_main_file";

    private final Directory directory;
    private final String fileName;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final ValueMerger<K, V> valueMerger;
    private final Props props;

    public SimpleDataFile(final Directory directory, final String fileName,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final ValueMerger<K, V> valueMerger) {
        this.directory = Objects.requireNonNull(directory);
        Objects.requireNonNull(fileName, "File name is required");
        if (fileName.contains(".")) {
            throw new IndexException(String.format(
                    "File name '%s' should not contain any dots.", fileName));
        }
        this.fileName = Objects.requireNonNull(fileName);
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
        this.valueMerger = Objects.requireNonNull(valueMerger);
        this.props = new Props(directory, fileName + ".properties");
    }

    public Stats getStats() {
        return new Stats(
                props.getLong(
                        PairWriterCountPair.NUMBER_OF_KEY_VALUE_PAIRS_IN_CACHE),
                props.getLong(NUMBER_OF_KEY_VALUE_PAIRS_IN_MAIN_FILE));
    }

    /**
     * Merge data from cache to main index.
     */
    public void compact() {
        long cx = 0;
        try (PairReaderIterator<K, V> iterator = new PairReaderIterator<>(
                openReader())) {
            try (SstFileWriter<K, V> writer = getTempFile().openWriter()) {
                while (iterator.hasNext()) {
                    writer.put(iterator.next());
                    cx++;
                }
            }
        }
        directory.deleteFile(getCacheFileName());
        directory.deleteFile(getMainFileName());
        directory.renameFile(getMergedFileName(), getMainFileName());
        props.setLong(PairWriterCountPair.NUMBER_OF_KEY_VALUE_PAIRS_IN_CACHE,
                0);
        props.setLong(NUMBER_OF_KEY_VALUE_PAIRS_IN_MAIN_FILE, cx);
        props.writeData();
    }

    /**
     * Return merged and sorted data from cache and main file. Access time to
     * reader could significantly slow down when number of data in cache.
     * 
     * @return reader with all data
     */
    public CloseablePairReader<K, V> openReader() {
        logger.debug("Opening of '{}' for reading started.", fileName);
        final CacheSortedReader<K, V> reader1 = new CacheSortedReader<>(
                valueMerger, getCacheFile(), keyTypeDescriptor.getComparator());
        final CloseablePairReader<K, V> reader2 = getMainFile().openReader();
        final MergedPairReader<K, V> mergedPairReader = new MergedPairReader<>(
                reader1, reader2, valueMerger,
                keyTypeDescriptor.getComparator());
        logger.debug("Opening of '{}' for reading is done.", fileName);
        return mergedPairReader;
    }

    private UnsortedDataFile<K, V> getCacheFile() {
        final UnsortedDataFile<K, V> out = UnsortedDataFile.<K, V>builder()
                .withDirectory(directory).withFileName(getCacheFileName())
                .withKeyReader(keyTypeDescriptor.getTypeReader())
                .withValueReader(valueTypeDescriptor.getTypeReader())
                .withKeyWriter(keyTypeDescriptor.getTypeWriter())
                .withValueWriter(valueTypeDescriptor.getTypeWriter()).build();
        return out;
    }

    /**
     * Split simple data file into two simple data files. Newly created simple
     * data file will contains smaller half of keys. Max key value in new simple
     * data file will be returned.
     * 
     * @param smallerDataFileName
     * @return
     */
    public K split(final String smallerDataFileName) {
        long cx = 0;
        long half = getStats().getTotalNumberOfPairs() / 2;
        K maxLowerIndexKey = null;
        try (PairIterator<K, V> iterator = new PairIteratorFromReader<>(
                openReader())) {
            final SimpleDataFile<K, V> sdfLower = new SimpleDataFile<>(
                    directory, smallerDataFileName, keyTypeDescriptor,
                    valueTypeDescriptor, valueMerger);
            maxLowerIndexKey = sdfLower.writePairsFromIterator(iterator, half);

            // read bigger half and store it to current simple data file.
            cx = 0;
            try (SstFileWriter<K, V> writer = getTempFile().openWriter()) {
                while (iterator.hasNext()) {
                    writer.put(iterator.next());
                    cx++;
                }
            }
            directory.deleteFile(getCacheFileName());
            directory.deleteFile(getMainFileName());
            directory.renameFile(getMergedFileName(), getMainFileName());
            props.setLong(
                    PairWriterCountPair.NUMBER_OF_KEY_VALUE_PAIRS_IN_CACHE, 0);
            props.setLong(NUMBER_OF_KEY_VALUE_PAIRS_IN_MAIN_FILE, cx);
            props.writeData();

        }
        return maxLowerIndexKey;
    }

    private SstFile<K, V> getMainFile() {
        final SstFile<K, V> out = getSortedFile(getMainFileName());
        return out;
    }

    private SstFile<K, V> getTempFile() {
        final SstFile<K, V> out = getSortedFile(getMergedFileName());
        return out;
    }

    private SstFile<K, V> getSortedFile(final String fileName) {
        final SstFile<K, V> out = SstFile.<K, V>builder()
                .withDirectory(directory).withFileName(fileName)
                .withKeyConvertorFromBytes(
                        keyTypeDescriptor.getConvertorFromBytes())
                .withValueReader(valueTypeDescriptor.getTypeReader())
                .withKeyConvertorToBytes(
                        keyTypeDescriptor.getConvertorToBytes())
                .withValueWriter(valueTypeDescriptor.getTypeWriter())
                .withKeyComparator(keyTypeDescriptor.getComparator()).build();
        return out;
    }

    /**
     * Writer ignores previously written data. Just append open data writer for
     * new data.
     * 
     * @return
     */
    public PairWriter<K, V> openCacheWriter() {
        final Access access = directory.isFileExists(getCacheFileName())
                ? Access.APPEND
                : Access.OVERWRITE;
        final PairWriter<K, V> basePairWriter = new UnsortedDataFileWriter<>(
                directory, getCacheFileName(),
                keyTypeDescriptor.getTypeWriter(),
                valueTypeDescriptor.getTypeWriter(), access);
        return new PairWriterCountPair<>(basePairWriter, props);
    }

    /**
     * Method overwrite main index file
     * 
     * @return
     */
    private K writePairsFromIterator(final PairIterator<K, V> iterator,
            long howManyToCopy) {
        K maxLowerIndexKey = null;
        long cx = 0;
        try (SstFileWriter<K, V> writer = getSortedFile(getMainFileName())
                .openWriter()) {
            while (cx < howManyToCopy) {
                final Pair<K, V> pair = iterator.next();
                writer.put(pair);
                maxLowerIndexKey = pair.getKey();
                cx++;
            }
        }
        props.setLong(NUMBER_OF_KEY_VALUE_PAIRS_IN_MAIN_FILE, cx);
        props.setLong(PairWriterCountPair.NUMBER_OF_KEY_VALUE_PAIRS_IN_CACHE,
                0);
        props.writeData();
        return maxLowerIndexKey;
    }

    private String getCacheFileName() {
        return fileName + ".unsorted";
    }

    private String getMainFileName() {
        return fileName + ".sorted";
    }

    private String getMergedFileName() {
        return fileName + ".merged";
    }

}
