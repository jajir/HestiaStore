package org.hestiastore.index.sst;

import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.hestiastore.index.Pair;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verify that put operation is immediately applied to all further results.
 * Changes should be applied even to already opened streams.
 * 
 * @author honza
 *
 */
class IntegrationIndexConsistencyTest extends AbstractIndexTest {
    private final Logger logger = LoggerFactory
            .getLogger(IntegrationIndexConsistencyTest.class);

    private static final int NUMBER_OF_TEST_PAIRS = 97;
    final Directory directory = new MemDirectory();
    final SegmentId id = SegmentId.of(27);
    final TypeDescriptorInteger tdi = new TypeDescriptorInteger();

    /**
     * Verify that what is written is read correctly back.
     * 
     * @
     */
    @Test
    void test_basic_consistency() {
        final Index<Integer, Integer> index = makeIndex();
        for (int i = 0; i < 100; i++) {
            writePairs(index, makeList(i));
            index.flush();
            verifyIndexData(index, makeList(i));
        }
    }

    /**
     * Test verify that read operation provide latest values. Even writing to
     * segment during
     * 
     * @
     */
    @Test
    void test_reading_of_updated_values() {
        final Index<Integer, Integer> index = makeIndex();
        writePairs(index, makeList(0));
        try (final Stream<Pair<Integer, Integer>> stream = index
                .getStream(SegmentWindow.unbounded())) {
            final AtomicInteger acx = new AtomicInteger();
            stream.forEach(pair -> {
                int cx = acx.incrementAndGet();
                writePairs(index, makeList(cx));
                logger.debug("{} {}", cx, pair);
                verifyIndexData(index, makeList(cx));
            });
        }
    }

    @Test
    void test_search_for_missing_key_bigger_than_last_existing_one() {
        final Index<Integer, Integer> index = makeIndex();
        writePairs(index, makeList(888));
        index.flush();
        for (int i = 0; i < NUMBER_OF_TEST_PAIRS; i++) {
            assertNull(index.get(i * 2 + 1));
        }
    }

    private Index<Integer, Integer> makeIndex() {
        final IndexConfiguration<Integer, Integer> conf = IndexConfiguration
                .<Integer, Integer>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(Integer.class)//
                .withKeyTypeDescriptor(tdi) //
                .withValueTypeDescriptor(tdi) //
                .withMaxNumberOfKeysInSegment(4) //
                .withMaxNumberOfKeysInSegmentCache(10L) //
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(12L)//
                .withMaxNumberOfKeysInSegmentIndexPage(2) //
                .withMaxNumberOfKeysInCache(3) //
                .withBloomFilterIndexSizeInBytes(0) //
                .withBloomFilterNumberOfHashFunctions(4) //
                .withLogEnabled(false) //
                .withName("test_index") //
                .build();
        return Index.<Integer, Integer>create(directory, conf);
    }

    protected List<Pair<Integer, Integer>> makeList(final int no) {
        final List<Pair<Integer, Integer>> out = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_TEST_PAIRS; i++) {
            out.add(Pair.of(i * 2, no));
        }
        return out;
    }

}
