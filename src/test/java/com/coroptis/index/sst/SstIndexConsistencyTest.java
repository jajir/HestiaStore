package com.coroptis.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

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
public class SstIndexConsistencyTest {

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
        final Index<Integer, Integer> seg = makeSstIndex();
        for (int i = 0; i < 100; i++) {
            writePairs(seg, makeList(i));
            verifyIndexData(seg, makeList(i));
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
        final Index<Integer, Integer> index = makeSstIndex();
        writePairs(index, makeList(0));
        try (final Stream<Pair<Integer, Integer>> stream = index.getStream()) {
            final AtomicInteger acx = new AtomicInteger();
            stream.forEach(pair -> {
                int cx = acx.incrementAndGet();
                writePairs(index, makeList(cx));
                System.out.println(cx + " " + pair.toString());
                verifyIndexData(index, makeList(cx));
            });
        }
    }

    private Index<Integer, Integer> makeSstIndex() {
        return makeSstIndex(false);
    }

    private Index<Integer, Integer> makeSstIndex(boolean withLog) {
        return Index.<Integer, Integer>builder().withDirectory(directory)
                .withKeyTypeDescriptor(tdi) //
                .withValueTypeDescriptor(tdi) //
                .withMaxNumberOfKeysInSegment(4) //
                .withMaxNumberOfKeysInSegmentCache(10000) //
                .withMaxNumberOfKeysInSegmentIndexPage(1000) //
                .withMaxNumberOfKeysInCache(2) //
                .withBloomFilterIndexSizeInBytes(1000) //
                .withBloomFilterNumberOfHashFunctions(4) //
                .withUseFullLog(withLog) //
                .build();
    }

    private List<Pair<Integer, Integer>> makeList(final int no) {
        final List<Pair<Integer, Integer>> out = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            out.add(Pair.of(i, no));
        }
        return out;
    }

    /**
     * Simplify filling segment with data.
     * 
     * @param <M>   key type
     * @param <N>   value type
     * @param seg   required segment
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
     * to expecetd value
     * 
     * @param <M>   key type
     * @param <N>   value type
     * @param index required segment
     * @param pairs required list of expected data in segment
     */
    protected <M, N> void verifyIndexData(final Index<M, N> index,
            final List<Pair<M, N>> pairs) {
        final List<Pair<M, N>> data = index.getStream()
                .collect(Collectors.toList());
        assertEquals(pairs.size(), data.size(), "Unexpected segment data size");
        for (int i = 0; i < pairs.size(); i++) {
            final Pair<M, N> expectedPair = pairs.get(i);
            final Pair<M, N> realPair = data.get(i);
            assertEquals(expectedPair, realPair);
        }
    }

}
