package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Validation;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Writing;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberValidation;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberWriting;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * When iterator is opened and data are changed during iterating than updated
 * data should be returned from already opened iterator.
 * 
 * @author honza
 *
 */
class IntegrationSegmentConsistencyTest extends AbstractSegmentTest {

    private static final int MAX_LOOP = 100;
    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final SegmentId id = SegmentId.of(29);
    private Directory dir;
    private Segment<Integer, Integer> seg;

    @BeforeEach
    void setUp() {
        dir = new MemDirectory();
        seg = Segment.<Integer, Integer>builder()//
                .withDirectory(dir)//
                .withId(id)//
                .withKeyTypeDescriptor(tdi)//
                .withValueTypeDescriptor(tdi)//
                .withMaxNumberOfKeysInSegmentCache(10000)//
                .withBloomFilterIndexSizeInBytes(0)//
                .withEncodingChunkFilters(//
                        List.of(new ChunkFilterMagicNumberWriting(), //
                                new ChunkFilterCrc32Writing(), //
                                new ChunkFilterDoNothing()//
                        ))//
                .withDecodingChunkFilters(//
                        List.of(new ChunkFilterMagicNumberValidation(), //
                                new ChunkFilterCrc32Validation(), //
                                new ChunkFilterDoNothing()//
                        ))//
                .build();
    }

    /**
     * Verify that what is written is read correctly back.
     * 
     * @
     */
    @Test
    void test_consistency() {
        for (int i = 0; i < MAX_LOOP; i++) {
            writePairs(seg, makeList(i));
            verifySegmentData(seg, makeList(i));
        }
    }

    /**
     * 
     * @
     */
    @Test
    void test_iterator_should_close_after_data_update() {
        writePairs(seg, makeList(0));
        final PairIterator<Integer, Integer> iterator = seg.openIterator();
        assertTrue(iterator.hasNext());
        assertEquals(Pair.of(0, 0), iterator.next());

        assertTrue(iterator.hasNext());
        assertEquals(Pair.of(1, 0), iterator.next());

        assertTrue(iterator.hasNext());
        assertEquals(Pair.of(2, 0), iterator.next());

        assertTrue(iterator.hasNext());
        assertEquals(Pair.of(3, 0), iterator.next());

        writePairs(seg, makeList(8));

        assertFalse(iterator.hasNext());
    }

    private List<Pair<Integer, Integer>> makeList(final int no) {
        final List<Pair<Integer, Integer>> out = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            out.add(Pair.of(i, no));
        }
        return out;
    }

}
