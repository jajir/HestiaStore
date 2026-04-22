package org.hestiastore.index.segmentindex.core.routing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import org.hestiastore.index.segmentindex.core.storage.IndexRecoveryCleanupCoordinator;
import org.hestiastore.index.segmentindex.core.maintenance.StableSegmentMaintenanceAccess;
import org.hestiastore.index.segmentindex.core.splitplanner.SplitPlanner;
import org.junit.jupiter.api.Test;

class SegmentIndexRuntimeSplitsTest {

    @Test
    void constructorRejectsNullBackgroundSplitCoordinator() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentIndexRuntimeSplits<>(null,
                        mock(StableSegmentMaintenanceAccess.class),
                        mock(SplitPlanner.class),
                        mock(DirectSegmentAccess.class),
                        mock(IndexRecoveryCleanupCoordinator.class)));

        assertEquals("Property 'backgroundSplitCoordinator' must not be null.",
                ex.getMessage());
    }

    @Test
    void exposesStoredSplitCollaborators() {
        final BackgroundSplitCoordinator<Integer, String> backgroundSplitCoordinator =
                mock(BackgroundSplitCoordinator.class);
        final StableSegmentMaintenanceAccess<Integer, String> stableSegmentCoordinator =
                mock(StableSegmentMaintenanceAccess.class);
        final SplitPlanner<Integer, String> splitPlanner =
                mock(SplitPlanner.class);
        final DirectSegmentAccess<Integer, String> directSegmentCoordinator =
                mock(DirectSegmentAccess.class);
        final IndexRecoveryCleanupCoordinator<Integer, String> recoveryCleanupCoordinator =
                mock(IndexRecoveryCleanupCoordinator.class);

        final SegmentIndexRuntimeSplits<Integer, String> state =
                new SegmentIndexRuntimeSplits<>(backgroundSplitCoordinator,
                        stableSegmentCoordinator, splitPlanner,
                        directSegmentCoordinator,
                        recoveryCleanupCoordinator);

        assertSame(backgroundSplitCoordinator,
                state.backgroundSplitCoordinator());
        assertSame(stableSegmentCoordinator, state.stableSegmentCoordinator());
        assertSame(splitPlanner, state.splitPlanner());
        assertSame(directSegmentCoordinator, state.directSegmentCoordinator());
        assertSame(recoveryCleanupCoordinator,
                state.recoveryCleanupCoordinator());
    }
}
