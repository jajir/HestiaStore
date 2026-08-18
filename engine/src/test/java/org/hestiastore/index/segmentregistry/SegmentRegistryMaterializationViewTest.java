package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.segment.SegmentFullWriterTx;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentRegistryMaterializationViewTest {

    @Mock
    private Supplier<SegmentId> segmentIdAllocator;

    @Mock
    private PreparedSegmentWriterFactory<Integer, String> preparedSegmentWriterFactory;

    @Mock
    private SegmentFullWriterTx<Integer, String> writerTx;

    private AtomicReference<SegmentRegistryState> registryState;
    private SegmentRegistryMaterializationView<Integer, String> view;

    @BeforeEach
    void setUp() {
        registryState = new AtomicReference<>(SegmentRegistryState.READY);
        view = new SegmentRegistryMaterializationView<>(segmentIdAllocator,
                preparedSegmentWriterFactory, registryState::get);
    }

    @Test
    void nextSegmentId_delegatesToAllocator() {
        final SegmentId expected = SegmentId.of(17);
        when(segmentIdAllocator.get()).thenReturn(expected);

        assertSame(expected, view.nextSegmentId());
    }

    @Test
    void nextSegmentId_rejectsNullAllocatorResult() {
        when(segmentIdAllocator.get()).thenReturn(null);

        assertThrows(IllegalArgumentException.class, view::nextSegmentId);
    }

    @ParameterizedTest
    @EnumSource(value = SegmentRegistryState.class, names = "READY",
            mode = EnumSource.Mode.EXCLUDE)
    void nextSegmentId_rejectsNonReadyRegistry(
            final SegmentRegistryState state) {
        registryState.set(state);

        assertThrows(IndexException.class, view::nextSegmentId);
        verifyNoInteractions(segmentIdAllocator);
    }

    @Test
    void openWriterTx_delegatesToFactory() {
        final SegmentId segmentId = SegmentId.of(9);
        when(preparedSegmentWriterFactory.openWriterTx(segmentId))
                .thenReturn(writerTx);

        assertSame(writerTx, view.openWriterTx(segmentId));
        verify(preparedSegmentWriterFactory).openWriterTx(segmentId);
    }

    @ParameterizedTest
    @EnumSource(value = SegmentRegistryState.class, names = "READY",
            mode = EnumSource.Mode.EXCLUDE)
    void openWriterTx_rejectsNonReadyRegistry(
            final SegmentRegistryState state) {
        registryState.set(state);

        assertThrows(IndexException.class,
                () -> view.openWriterTx(SegmentId.of(9)));
        verifyNoInteractions(preparedSegmentWriterFactory);
    }

    @Test
    void constructorRejectsNullAllocator() {
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentRegistryMaterializationView<Integer, String>(
                        null, preparedSegmentWriterFactory,
                        registryState::get));
    }

    @Test
    void constructorRejectsNullPreparedSegmentWriterFactory() {
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentRegistryMaterializationView<Integer, String>(
                        segmentIdAllocator, null, registryState::get));
    }

    @Test
    void constructorRejectsNullRegistryStateSupplier() {
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentRegistryMaterializationView<Integer, String>(
                        segmentIdAllocator, preparedSegmentWriterFactory,
                        null));
    }
}
