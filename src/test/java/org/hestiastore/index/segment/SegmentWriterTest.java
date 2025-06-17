package org.hestiastore.index.segment;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentWriterTest {

    private static final Pair<Integer, String> PAIR_1 = Pair.of(1, "aaa");
    private static final Pair<Integer, String> PAIR_2 = Pair.of(2, "bbb");
    private static final Pair<Integer, String> PAIR_3 = Pair.of(3, "ccc");

    @Mock
    private SegmentCompacter<Integer, String> segmentCompacter;

    @Mock
    private SegmentDeltaCacheController<Integer, String> deltaCacheController;

    @Mock
    private SegmentDeltaCacheWriter<Integer, String> deltaCacheWriter;

    @Test
    void test_basic_writing() {
        when(deltaCacheController.openWriter()).thenReturn(deltaCacheWriter);
        when(deltaCacheWriter.getNumberOfKeys()).thenReturn(1L, 2L, 3L);
        when(segmentCompacter.shouldBeCompactedDuringWriting(1))
                .thenReturn(false);
        when(segmentCompacter.shouldBeCompactedDuringWriting(2))
                .thenReturn(false);
        when(segmentCompacter.shouldBeCompactedDuringWriting(3))
                .thenReturn(false);
        try (final PairWriter<Integer, String> writer = new SegmentWriter<>(
                segmentCompacter, deltaCacheController)) {
            writer.put(PAIR_1);
            writer.put(PAIR_2);
            writer.put(PAIR_3);
        }

        // verify that writing to cache delta file name was done
        verify(deltaCacheWriter).put(PAIR_1);
        verify(deltaCacheWriter).put(PAIR_2);
        verify(deltaCacheWriter).put(PAIR_3);

        verify(segmentCompacter).shouldBeCompactedDuringWriting(1);
        verify(segmentCompacter).shouldBeCompactedDuringWriting(2);
        verify(segmentCompacter).shouldBeCompactedDuringWriting(3);
    }

    @Test
    void test_compact_during_writing() {
        when(deltaCacheController.openWriter()).thenReturn(deltaCacheWriter);
        when(deltaCacheWriter.getNumberOfKeys()).thenReturn(1L, 2L, 3L);
        when(segmentCompacter.shouldBeCompactedDuringWriting(1))
                .thenReturn(false);
        when(segmentCompacter.shouldBeCompactedDuringWriting(2))
                .thenReturn(true);
        when(segmentCompacter.shouldBeCompactedDuringWriting(3))
                .thenReturn(false);
        try (final PairWriter<Integer, String> writer = new SegmentWriter<>(
                segmentCompacter, deltaCacheController)) {
            writer.put(PAIR_1);
            writer.put(PAIR_2);
            writer.put(PAIR_3);
        }

        // verify that writing to cache delta file name was done
        verify(deltaCacheWriter).put(PAIR_1);
        verify(deltaCacheWriter).put(PAIR_2);
        verify(deltaCacheWriter).put(PAIR_3);

        verify(segmentCompacter).shouldBeCompactedDuringWriting(1);
        verify(segmentCompacter).shouldBeCompactedDuringWriting(2);
        verify(segmentCompacter).shouldBeCompactedDuringWriting(3);

        verify(segmentCompacter, times(1)).forceCompact();
    }

}
