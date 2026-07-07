package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.segment.Segment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentRegistryEntryTest {

    @Mock
    private Segment<Integer, String> segment;

    @Mock
    private Segment<Integer, String> otherSegment;

    @Test
    void transitionsFromLoadingToReadyToUnloading() {
        final SegmentRegistryEntry<Integer, String> entry = new SegmentRegistryEntry<>(
                7L);

        assertTrue(entry.tryStartLoad());

        entry.finishLoad(segment);
        assertEquals(segment, entry.waitWhileLoading(8L));

        assertTrue(entry.tryStartUnload(segment));
        assertEquals(segment, entry.getValueForUnload());
        entry.finishUnload();

        assertThrows(SegmentBusyException.class,
                () -> entry.waitWhileLoading(9L));
    }

    @Test
    void invalidTransitionsFailPredictably() {
        final SegmentRegistryEntry<Integer, String> entry = new SegmentRegistryEntry<>(
                1L);

        entry.finishLoad(segment);

        assertThrows(IndexException.class, () -> entry.finishLoad(otherSegment));
        final IndexException failure = new IndexException("boom");
        assertThrows(IndexException.class, () -> entry.fail(failure));
        assertThrows(IndexException.class, entry::finishUnload);
    }
}
