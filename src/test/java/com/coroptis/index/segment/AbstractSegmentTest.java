package com.coroptis.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.PairWriter;
import com.coroptis.index.directory.Directory;

public abstract class AbstractSegmentTest {

    private final Logger logger = LoggerFactory
            .getLogger(AbstractSegmentTest.class);

    /**
     * Simplify filling segment with data.
     * 
     * @param <M>   key type
     * @param <N>   value type
     * @param seg   required segment
     * @param pairs required list of pairs
     */
    protected <M, N> void writePairs(final Segment<M, N> seg,
            final List<Pair<M, N>> pairs) {
        try (PairWriter<M, N> writer = seg.openWriter()) {
            for (final Pair<M, N> pair : pairs) {
                writer.put(pair);
            }
        }
    }

    /**
     * Convert pair iterator data to list
     * 
     * @param <M>      key type
     * @param <N>      value type
     * @param iterator
     * @returnlist of pairs with data from list
     */
    protected <M, N> List<Pair<M, N>> toList(
            final PairIterator<M, N> iterator) {
        final ArrayList<Pair<M, N>> out = new ArrayList<>();
        while (iterator.hasNext()) {
            out.add(iterator.next());
        }
        iterator.close();
        return out;
    }

    /**
     * Open segment search and verify that found value for given key is equals
     * to expected value
     * 
     * @param <M>   key type
     * @param <N>   value type
     * @param seg   required segment
     * @param pairs required list of pairs of key and expected value
     */
    protected <M, N> void verifySegmentSearch(final Segment<M, N> seg,
            final List<Pair<M, N>> pairs) {
        pairs.forEach(pair -> {
            final M key = pair.getKey();
            final N expectedValue = pair.getValue();
            assertEquals(expectedValue, seg.get(key));
        });
    }

    /**
     * Open segment search and verify that found value for given key is equals
     * to expecetd value
     * 
     * @param <M>   key type
     * @param <N>   value type
     * @param seg   required segment
     * @param pairs required list of expected data in segment
     */
    protected <M, N> void verifySegmentData(final Segment<M, N> seg,
            final List<Pair<M, N>> pairs) {
        final List<Pair<M, N>> data = toList(seg.openIterator());
        assertEquals(pairs.size(), data.size(), "Unexpected segment data size");
        for (int i = 0; i < pairs.size(); i++) {
            final Pair<M, N> expectedPair = pairs.get(i);
            final Pair<M, N> realPair = data.get(i);
            assertEquals(expectedPair, realPair);
        }
    }

    protected int numberOfFilesInDirectory(final Directory directory) {
        return (int) directory.getFileNames().count();
    }

    protected int numberOfFilesInDirectoryP(final Directory directory) {
        final AtomicInteger cx = new AtomicInteger(0);
        directory.getFileNames().forEach(fileName -> {
            logger.debug("Found file name {}", fileName);
            cx.incrementAndGet();
        });
        return cx.get();
    }

}
