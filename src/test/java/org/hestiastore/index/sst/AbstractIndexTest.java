package org.hestiastore.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.hestiastore.index.AbstractDataTest;
import org.hestiastore.index.Entry;
import org.hestiastore.index.directory.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractIndexTest extends AbstractDataTest {

    private final Logger logger = LoggerFactory
            .getLogger(AbstractIndexTest.class);

    /**
     * Simplify filling index with data.
     * 
     * @param <M>   key type
     * @param <N>   value type
     * @param seg   required index
     * @param entries required list of entries
     */
    protected <M, N> void writeEntries(final Index<M, N> index,
            final List<Entry<M, N>> entries) {
        for (final Entry<M, N> entry : entries) {
            index.put(entry);
        }
    }

    /**
     * Open segment search and verify that found value for given key is equals
     * to expected value
     * 
     * @param <M>   key type
     * @param <N>   value type
     * @param seg   required segment
     * @param entries required list of entries of key and expected value
     */
    protected <M, N> void verifyIndexSearch(final Index<M, N> index,
            final List<Entry<M, N>> entries) {
        entries.forEach(entry -> {
            final M key = entry.getKey();
            final N expectedValue = entry.getValue();
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
     * @param entries required list of expected data in index
     */
    protected <M, N> void verifyIndexData(final Index<M, N> index,
            final List<Entry<M, N>> entries) {
        final List<Entry<M, N>> data = toList(
                index.getStream(SegmentWindow.unbounded()));
        assertEquals(entries.size(), data.size(),
                "Unexpected number of entries in index");
        for (int i = 0; i < entries.size(); i++) {
            final Entry<M, N> expectedPair = entries.get(i);
            final Entry<M, N> realPair = data.get(i);
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
