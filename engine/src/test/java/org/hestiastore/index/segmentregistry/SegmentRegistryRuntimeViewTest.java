package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.segment.SegmentRuntimeLimits;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentRegistryRuntimeViewTest {

    @Mock
    private Consumer<SegmentRuntimeLimits> runtimeTuner;

    @Mock
    private BlockingSegment<Integer, String> firstSegment;

    @Mock
    private BlockingSegment<Integer, String> secondSegment;

    @Mock
    private Supplier<List<BlockingSegment<Integer, String>>> loadedSegmentsSnapshot;

    private AtomicReference<SegmentRegistryState> registryState;
    private SegmentRegistryRuntimeView<Integer, String> view;

    @BeforeEach
    void setUp() {
        registryState = new AtomicReference<>(SegmentRegistryState.READY);
        view = new SegmentRegistryRuntimeView<>(runtimeTuner,
                loadedSegmentsSnapshot, registryState::get);
    }

    @Test
    void updateRuntimeLimits_delegatesToRuntimeTuner() {
        final SegmentRuntimeLimits limits = new SegmentRuntimeLimits(10, 5, 7);

        view.updateRuntimeLimits(limits);

        verify(runtimeTuner).accept(limits);
    }

    @ParameterizedTest
    @EnumSource(value = SegmentRegistryState.class, names = "READY",
            mode = EnumSource.Mode.EXCLUDE)
    void updateRuntimeLimits_rejectsNonReadyRegistry(
            final SegmentRegistryState state) {
        registryState.set(state);

        assertThrows(IndexException.class,
                () -> view.updateRuntimeLimits(
                        new SegmentRuntimeLimits(10, 5, 7)));
        verifyNoInteractions(runtimeTuner);
    }

    @Test
    void loadedSegmentsSnapshot_returnsDelegatedSnapshot() {
        final List<BlockingSegment<Integer, String>> snapshot = List.of(
                firstSegment,
                secondSegment);
        when(loadedSegmentsSnapshot.get()).thenReturn(snapshot);

        assertSame(snapshot, view.loadedSegmentsSnapshot());
    }

    @ParameterizedTest
    @EnumSource(value = SegmentRegistryState.class, names = "READY",
            mode = EnumSource.Mode.EXCLUDE)
    void loadedSegmentsSnapshot_returnsEmptyWhenRegistryIsNotReady(
            final SegmentRegistryState state) {
        registryState.set(state);

        assertEquals(List.of(), view.loadedSegmentsSnapshot());
        verifyNoInteractions(loadedSegmentsSnapshot);
    }

    @Test
    void constructorRejectsNullRuntimeTuner() {
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentRegistryRuntimeView<Integer, String>(null,
                        loadedSegmentsSnapshot, registryState::get));
    }

    @Test
    void constructorRejectsNullLoadedSegmentsSnapshotSupplier() {
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentRegistryRuntimeView<Integer, String>(
                        runtimeTuner, null, registryState::get));
    }

    @Test
    void constructorRejectsNullRegistryStateSupplier() {
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentRegistryRuntimeView<Integer, String>(
                        runtimeTuner, loadedSegmentsSnapshot, null));
    }
}
