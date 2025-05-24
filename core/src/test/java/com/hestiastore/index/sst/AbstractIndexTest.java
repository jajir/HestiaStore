package com.hestiastore.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hestiastore.index.AbstractDataTest;
import com.hestiastore.index.Pair;
import com.hestiastore.index.directory.Directory;

public abstract class AbstractIndexTest extends AbstractDataTest {

    private final Logger logger = LoggerFactory
            .getLogger(AbstractIndexTest.class);

    /**
     * Simplify filling index with data.
     * 
     * @param <M>   key type
     * @param <N>   value type
     * @param seg   required index
     * @param pairs required list of pairs
     */
    protected <M, N> void writePairs(final Index<M, N> index,
            final List<Pair<M, N>> pairs) {
        for (final Pair<M, N> pair : pairs) {
            index.put(pair);
        }
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
    protected <M, N> void verifyIndexSearch(final Index<M, N> index,
            final List<Pair<M, N>> pairs) {
        pairs.forEach(pair -> {
            final M key = pair.getKey();
            final N expectedValue = pair.getValue();
            assertEquals(expectedValue, index.get(key));
        });
    }

    /**
     * Open index search and verify that found value for given key is equals to
     * expecetd value
     * 
     * @param <M>   key type
     * @param <N>   value type
     * @param seg   required index
     * @param pairs required list of expected data in index
     */
    protected <M, N> void verifyIndexData(final Index<M, N> index,
            final List<Pair<M, N>> pairs) {
        final List<Pair<M, N>> data = toList(
                index.getStream(SegmentWindow.unbounded()));
        assertEquals(pairs.size(), data.size(),
                "Unexpected number of pairs in index");
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
