package org.hestiastore.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class for data tests
 * 
 * Don't extend it, use static imports.
 * 
 */
public abstract class AbstractDataTest {

    private final static Logger LOGGER = LoggerFactory
            .getLogger(AbstractDataTest.class);

    /**
     * Convert entry iterator data to list
     * 
     * @param <M>      key type
     * @param <N>      value type
     * @param iterator
     * @returnlist of entries with data from list
     */
    protected <M, N> List<Entry<M, N>> toList(
            final Stream<Entry<M, N>> iterator) {
        final ArrayList<Entry<M, N>> out = new ArrayList<>();
        iterator.forEach(entry -> out.add(entry));
        return out;
    }

    /**
     * Compare two key value entries.
     * 
     * @param expectedPair expected entry
     * @param entry        verified entry
     */
    protected void verifyEquals(final Entry<String, Integer> expectedPair,
            final Entry<String, Integer> entry) {
        assertNotNull(expectedPair);
        assertNotNull(entry);
        assertEquals(expectedPair.getKey(), entry.getKey());
        assertEquals(expectedPair.getValue(), entry.getValue());
    }

    /**
     * Convert entry iterator data to list
     * 
     * @param <M>      key type
     * @param <N>      value type
     * @param iterator
     * @returnlist of entries with data from list
     */
    protected static <M, N> List<Entry<M, N>> toList(
            final EntryIterator<M, N> iterator) {
        final ArrayList<Entry<M, N>> out = new ArrayList<>();
        while (iterator.hasNext()) {
            out.add(iterator.next());
        }
        iterator.close();
        return out;
    }

    /**
     * Verify that data from iterator are same as expecetd values
     * 
     * @param <M>           key type
     * @param <N>           value type
     * @param entries       required list of expected data in segment
     * @param entryIterator required entry iterator
     */
    public static <M, N> void verifyIteratorData(
            final List<Entry<M, N>> entries,
            final EntryIterator<M, N> entryIterator) {
        final List<Entry<M, N>> data = toList(entryIterator);
        assertEquals(entries.size(), data.size(),
                "Unexpected iterator data size");
        for (int i = 0; i < entries.size(); i++) {
            final Entry<M, N> expectedPair = entries.get(i);
            final Entry<M, N> realPair = data.get(i);
            assertEquals(expectedPair, realPair);
        }
    }

    /**
     * Verify that data from iterator are same as expected values, using a
     * SegmentResult wrapper.
     *
     * @param <M>      key type
     * @param <N>      value type
     * @param entries  required list of expected data in segment
     * @param result   required segment result with iterator
     */
    public static <M, N> void verifyIteratorData(
            final List<Entry<M, N>> entries,
            final SegmentResult<EntryIterator<M, N>> result) {
        assertNotNull(result);
        assertEquals(SegmentResultStatus.OK, result.getStatus(),
                "Expected iterator result OK");
        assertNotNull(result.getValue());
        verifyIteratorData(entries, result.getValue());
    }

    /**
     * Convert segment data to list.
     * 
     * @param segment required segment
     * @return list of entries with data from segment
     */
    public static List<Entry<Integer, String>> segmentToList(
            final Segment<Integer, String> segment) {
        final SegmentResult<EntryIterator<Integer, String>> result = segment
                .openIterator();
        assertEquals(SegmentResultStatus.OK, result.getStatus(),
                "Expected iterator result OK");
        assertNotNull(result.getValue());
        try (EntryIterator<Integer, String> iterator = result.getValue()) {
            final List<Entry<Integer, String>> out = new ArrayList<>();
            iterator.forEachRemaining(out::add);
            return out;
        }
    }

    /**
     * Verifies expected count of files in directory and logs the file list when
     * the count does not match.
     *
     * @param directory             required directory to inspect
     * @param expecetdNumberOfFiles expected number of files in directory
     */
    public static void verifyNumberOfFiles(final Directory directory,
            final int expecetdNumberOfFiles) {
        final List<String> fileNames = directory.getFileNames().toList();
        final int fileCount = fileNames.size();
        if (fileCount != expecetdNumberOfFiles) {
            LOGGER.error("Unexpected files in directory: {}", fileNames);
        }
        assertEquals(expecetdNumberOfFiles, fileCount,
                "Invalid numbe of files in directory");
    }

}
