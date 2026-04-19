package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;

import java.util.List;

import org.hestiastore.index.segment.SegmentRuntimeLimits;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentRegistryRuntimeViewTest {

    @Mock
    private SegmentRuntimeTuner runtimeTuner;

    @Mock
    private SegmentHandle<Integer, String> firstSegment;

    @Mock
    private SegmentHandle<Integer, String> secondSegment;

    @Test
    void updateRuntimeLimits_delegatesToRuntimeTuner() {
        final SegmentRegistryRuntimeView<Integer, String> view = new SegmentRegistryRuntimeView<>(
                runtimeTuner, () -> List.of());
        final SegmentRuntimeLimits limits = new SegmentRuntimeLimits(10, 5, 7);

        view.updateRuntimeLimits(limits);

        verify(runtimeTuner).updateRuntimeLimits(limits);
    }

    @Test
    void loadedSegmentsSnapshot_returnsDelegatedSnapshot() {
        final List<SegmentHandle<Integer, String>> snapshot = List.of(
                firstSegment,
                secondSegment);
        final SegmentRegistryRuntimeView<Integer, String> view = new SegmentRegistryRuntimeView<>(
                runtimeTuner, () -> snapshot);

        assertSame(snapshot, view.loadedSegmentsSnapshot());
    }

    @Test
    void constructorRejectsNullRuntimeTuner() {
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentRegistryRuntimeView<Integer, String>(null,
                        List::<SegmentHandle<Integer, String>>of));
    }

    @Test
    void constructorRejectsNullLoadedSegmentsSnapshotSupplier() {
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentRegistryRuntimeView<Integer, String>(
                        runtimeTuner, null));
    }
}
