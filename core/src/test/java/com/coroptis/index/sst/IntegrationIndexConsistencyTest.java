package com.coroptis.index.sst;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.Pair;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;
import com.coroptis.index.segment.SegmentId;

/**
 * Verify that put operation is immediately applied to all further results.
 * Changes should be applied even to already opened streams.
 * 
 * @author honza
 *
 */
public class IntegrationIndexConsistencyTest extends AbstractIndexTest {
    private final Logger logger = LoggerFactory
            .getLogger(IntegrationIndexConsistencyTest.class);

    final Directory directory = new MemDirectory();
    final SegmentId id = SegmentId.of(27);
    final TypeDescriptorInteger tdi = new TypeDescriptorInteger();

    /**
     * Verify that what is written is read correctly back.
     * 
     * @throws Exception
     */
    @Test
    void test_basic_consistency() throws Exception {
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
     * @throws Exception
     */
    @Test
    void test_reading_of_updated_values() throws Exception {
        final Index<Integer, Integer> index = makeIndex();
        writePairs(index, makeList(0));
        try (final Stream<Pair<Integer, Integer>> stream = index.getStream()) {
            final AtomicInteger acx = new AtomicInteger();
            stream.forEach(pair -> {
                int cx = acx.incrementAndGet();
                writePairs(index, makeList(cx));
                logger.debug(cx + " " + pair.toString());
                verifyIndexData(index, makeList(cx));
            });
        }
    }

    private Index<Integer, Integer> makeIndex() {
        return Index.<Integer, Integer>builder()//
                .withDirectory(directory)//
                .withKeyClass(Integer.class)//
                .withValueClass(Integer.class)//
                .withKeyTypeDescriptor(tdi) //
                .withValueTypeDescriptor(tdi) //
                .withCustomConf()//
                .withMaxNumberOfKeysInSegment(4) //
                .withMaxNumberOfKeysInSegmentCache(10000) //
                .withMaxNumberOfKeysInSegmentIndexPage(1000) //
                .withMaxNumberOfKeysInCache(2) //
                .withBloomFilterIndexSizeInBytes(1000) //
                .withBloomFilterNumberOfHashFunctions(4) //
                .withUseFullLog(false) //
                .build();
    }

    protected List<Pair<Integer, Integer>> makeList(final int no) {
        final List<Pair<Integer, Integer>> out = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            out.add(Pair.of(i, no));
        }
        return out;
    }

}
