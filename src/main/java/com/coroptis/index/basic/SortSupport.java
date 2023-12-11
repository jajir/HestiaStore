package com.coroptis.index.basic;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.ValueMerger;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.sstfile.SstFile;
import com.coroptis.index.sstfile.SstFileWriter;

public class SortSupport<K, V> {

    private final static String ROUND_SEPARTOR = "-";

    private static final Logger LOGGER = Logger
            .getLogger(SortSupport.class.getName());

    private final Directory directory;
    private final BasicIndex<K, V> basicIndex;
    private final ValueMerger<K, V> merger;
    private final String fileName;

    public SortSupport(final BasicIndex<K, V> basicIndex,
            final ValueMerger<K, V> merger, final String fileName) {
        this.basicIndex = Objects.requireNonNull(basicIndex);
        this.merger = Objects.requireNonNull(merger);
        this.fileName = Objects.requireNonNull(fileName);
        this.directory = basicIndex.getDirectory();
    }

    public List<String> getFilesInRound(final int roundNo) {
        return directory.getFileNames()
                .filter(fileName -> isFileInRound(roundNo, fileName)).sorted()
                .collect(Collectors.toCollection(() -> new ArrayList<>()));
    }

    private boolean isFileInRound(final int roundNo, final String fileName) {
        final String[] parts = fileName.split(ROUND_SEPARTOR);
        if (parts.length == 3) {
            int currentRoundNo = Integer.parseInt(parts[1]);
            return roundNo == currentRoundNo;
        } else {
            return false;
        }
    }

    public String makeFileName(final int roundNo, final int no) {
        final int positionOfDot = fileName.lastIndexOf(".");
        if (positionOfDot > 0) {
            final String extension = fileName.substring(positionOfDot);
            final String firstPart = fileName.substring(0, positionOfDot);
            return firstPart + ROUND_SEPARTOR + roundNo + ROUND_SEPARTOR + no
                    + extension;
        } else {
            return fileName + ROUND_SEPARTOR + roundNo + ROUND_SEPARTOR + no;
        }
    }

    void mergeSortedFiles(final List<String> filesToMergeLocaly,
            final String producedFile) {
        final SstFile<K, V> sortedFile = basicIndex
                .getSortedDataFile(producedFile);
        try (final SstFileWriter<K, V> indexWriter = sortedFile.openWriter()) {
            mergeSortedFiles(filesToMergeLocaly, pair -> indexWriter.put(pair));
        }
    }

    void mergeSortedFiles(final List<String> filesToMergeLocaly,
            final Consumer<Pair<K, V>> consumer) {
        List<PairIterator<K, V>> readers = null;
        try {
            readers = filesToMergeLocaly
                    .stream().map(fileName -> basicIndex
                            .getSortedDataFile(fileName).openIterator())
                    .collect(Collectors.toList());
            final MergeSpliterator<K, V> mergeSpliterator = new MergeSpliterator<K, V>(
                    readers, basicIndex.getKeyComparator(), merger);
            final Stream<Pair<K, V>> pairStream = StreamSupport
                    .stream(mergeSpliterator, false);
            pairStream.forEach(pair -> consumer.accept(pair));
        } finally {
            if (readers != null) {
                readers.forEach(reader -> {
                    try {
                        reader.close();
                    } catch (Exception e) {
                        // Just closing all readers, when exceptions occurs I
                        // don't care.
                        LOGGER.info(() -> String.format(
                                "Unable to close DataFileIterator '%s'. "
                                        + "This doesn't have to be error.",
                                reader));
                    }
                });
            }
        }
    }

}
