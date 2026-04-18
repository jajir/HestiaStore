package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.segment.SegmentFullWriterTx;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentRegistryMaterializationViewTest {

    @Mock
    private SegmentIdAllocator segmentIdAllocator;

    @Mock
    private PreparedSegmentWriterFactory<Integer, String> preparedSegmentWriterFactory;

    @Mock
    private SegmentFullWriterTx<Integer, String> writerTx;

    @Test
    void nextSegmentId_delegatesToAllocator() {
        final SegmentId expected = SegmentId.of(17);
        when(segmentIdAllocator.nextId()).thenReturn(expected);
        final SegmentRegistryMaterializationView<Integer, String> view = new SegmentRegistryMaterializationView<>(
                segmentIdAllocator, preparedSegmentWriterFactory);

        assertSame(expected, view.nextSegmentId());
    }

    @Test
    void nextSegmentId_rejectsNullAllocatorResult() {
        when(segmentIdAllocator.nextId()).thenReturn(null);
        final SegmentRegistryMaterializationView<Integer, String> view = new SegmentRegistryMaterializationView<>(
                segmentIdAllocator, preparedSegmentWriterFactory);

        assertThrows(IllegalArgumentException.class, view::nextSegmentId);
    }

    @Test
    void openWriterTx_delegatesToFactory() {
        final SegmentId segmentId = SegmentId.of(9);
        when(preparedSegmentWriterFactory.openWriterTx(segmentId))
                .thenReturn(writerTx);
        final SegmentRegistryMaterializationView<Integer, String> view = new SegmentRegistryMaterializationView<>(
                segmentIdAllocator, preparedSegmentWriterFactory);

        assertSame(writerTx, view.openWriterTx(segmentId));
        verify(preparedSegmentWriterFactory).openWriterTx(segmentId);
    }

    @Test
    void constructorRejectsNullAllocator() {
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentRegistryMaterializationView<Integer, String>(
                        null, preparedSegmentWriterFactory));
    }

    @Test
    void constructorRejectsNullPreparedSegmentWriterFactory() {
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentRegistryMaterializationView<Integer, String>(
                        segmentIdAllocator, null));
    }
}
