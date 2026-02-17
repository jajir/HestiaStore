package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.hestiastore.index.AbstractDataTest;
import org.hestiastore.index.Entry;
import org.hestiastore.index.directory.Directory;

public abstract class AbstractSegmentTest extends AbstractDataTest {

    /**
     * Simplify filling segment with data.
     * 
     * @param <M>     key type
     * @param <N>     value type
     * @param seg     required segment
     * @param entries required list of entries
     */
    protected <M, N> void writeEntries(final Segment<M, N> seg,
            final List<Entry<M, N>> entries) {
        for (final Entry<M, N> entry : entries) {
            assertEquals(SegmentResultStatus.OK,
                    seg.put(entry.getKey(), entry.getValue()).getStatus());
        }
        assertEquals(SegmentResultStatus.OK, seg.flush().getStatus());
    }

    /**
     * Open segment search and verify that found value for given key is equals
     * to expected value
     * 
     * @param <M>     key type
     * @param <N>     value type
     * @param seg     required segment
     * @param entries required list of entries of key and expected value
     */
    protected <M, N> void verifySegmentSearch(final Segment<M, N> seg,
            final List<Entry<M, N>> entries) {
        entries.forEach(entry -> {
            final M key = entry.getKey();
            final N expectedValue = entry.getValue();
            final SegmentResult<N> result = seg.get(key);
            assertEquals(SegmentResultStatus.OK, result.getStatus());
            assertEquals(expectedValue, result.getValue(),
                    String.format("Unable to find value for key '%s'.", key));
        });
    }

    /**
     * Open segment search and verify that found value for given key is equals
     * to expecetd value
     * 
     * @param <M>     key type
     * @param <N>     value type
     * @param seg     required segment
     * @param entries required list of expected data in segment
     */
    public static <M, N> void verifySegmentData(final Segment<M, N> seg,
            final List<Entry<M, N>> entries) {
        verifyIteratorData(entries, seg.openIterator());
    }

    protected int numberOfFilesInDirectory(final Directory directory) {
        return (int) directory.getFileNames()
                .filter(name -> name.contains("."))
                .filter(name -> !name.endsWith(".lock")).count();
    }

    protected void verifyCacheFiles(final Directory directory) {
        long cacheFileCount = directory.getFileNames()
                .filter(fileName -> fileName.endsWith(".cache")).count();
        assertEquals(0, cacheFileCount,
                "Expected zero .cache files in directory");
    }

}
