package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.hestiastore.index.Entry;
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
class IntegrationSegmentIndexConsistencyTest extends AbstractSegmentIndexTest {
    private final Logger logger = LoggerFactory
            .getLogger(IntegrationSegmentIndexConsistencyTest.class);

    private static final int NUMBER_OF_TEST_ENTRIES = 97;
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
        final SegmentIndex<Integer, Integer> index = makeIndex();
        for (int i = 0; i < 100; i++) {
            writeEntries(index, makeList(i));
            index.flush();
            awaitMaintenanceIdle(index);
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
        final SegmentIndex<Integer, Integer> index = makeIndex();
        writeEntries(index, makeList(0));
        try (final Stream<Entry<Integer, Integer>> stream = index
                .getStream(SegmentWindow.unbounded())) {
            final AtomicInteger acx = new AtomicInteger();
            stream.forEach(entry -> {
                int cx = acx.incrementAndGet();
                writeEntries(index, makeList(cx));
                logger.debug("{} {}", cx, entry);
                verifyIndexData(index, makeList(cx));
            });
        }
    }

    @Test
    void test_search_for_missing_key_bigger_than_last_existing_one() {
        final SegmentIndex<Integer, Integer> index = makeIndex();
        writeEntries(index, makeList(888));
        index.flush();
        awaitMaintenanceIdle(index);
        for (int i = 0; i < NUMBER_OF_TEST_ENTRIES; i++) {
            assertNull(index.get(i * 2 + 1));
        }
    }

    private SegmentIndex<Integer, Integer> makeIndex() {
        final IndexConfiguration<Integer, Integer> conf = IndexConfiguration
                .<Integer, Integer>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(Integer.class)//
                .withKeyTypeDescriptor(tdi) //
                .withValueTypeDescriptor(tdi) //
                .withMaxNumberOfKeysInSegmentCache(10) //
                .withMaxNumberOfKeysInSegment(4) //
                .withMaxNumberOfKeysInSegmentChunk(2) //
                .withMaxNumberOfKeysInCache(3) //
                .withBloomFilterIndexSizeInBytes(0) //
                .withBloomFilterNumberOfHashFunctions(4) //
                .withContextLoggingEnabled(false) //
                .withName("test_index") //
                .build();
        return SegmentIndex.<Integer, Integer>create(directory, conf);
    }

    protected List<Entry<Integer, Integer>> makeList(final int no) {
        final List<Entry<Integer, Integer>> out = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_TEST_ENTRIES; i++) {
            out.add(Entry.of(i * 2, no));
        }
        return out;
    }

}
