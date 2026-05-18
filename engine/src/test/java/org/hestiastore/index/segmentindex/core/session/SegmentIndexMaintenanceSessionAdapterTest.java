package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.hestiastore.index.segmentindex.maintenance.SegmentIndexMaintenance;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentIndexMaintenanceSessionAdapterTest {

    @Mock
    private SegmentIndexMaintenance delegate;

    @Mock
    private SegmentIndexSessionOwner<Integer, String> sessionOwner;

    @Mock
    private SegmentIndexTrackedOperationRunner<Integer, String> trackedRunner;

    @Test
    void maintenanceCommandsRunThroughSessionAndTrackingBoundaries() {
        enablePassThroughBoundaries();
        final SegmentIndexMaintenance adapter =
                new SegmentIndexMaintenanceSessionAdapter<>(delegate,
                        sessionOwner, trackedRunner);

        assertDoesNotThrow(() -> {
            adapter.compact();
            adapter.compactAndWait();
            adapter.flush();
            adapter.flushAndWait();
            adapter.checkAndRepairConsistency();
        });

        verify(delegate).compact();
        verify(delegate).compactAndWait();
        verify(delegate).flush();
        verify(delegate).flushAndWait();
        verify(delegate).checkAndRepairConsistency();
        verify(sessionOwner, times(5)).runMaintenanceOperation(
                any(Runnable.class));
        verify(trackedRunner, times(5)).runTrackedVoid(any(Runnable.class));
        verifyNoMoreInteractions(delegate, sessionOwner, trackedRunner);
    }

    @Test
    void constructorRejectsMissingCollaborators() {
        assertDoesNotThrow(() -> new SegmentIndexMaintenanceSessionAdapter<>(
                delegate, sessionOwner, trackedRunner));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentIndexMaintenanceSessionAdapter<>(null,
                        sessionOwner, trackedRunner));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentIndexMaintenanceSessionAdapter<>(delegate,
                        null, trackedRunner));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentIndexMaintenanceSessionAdapter<>(delegate,
                        sessionOwner, null));
    }

    private void enablePassThroughBoundaries() {
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(sessionOwner).runMaintenanceOperation(any(Runnable.class));
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(trackedRunner).runTrackedVoid(any(Runnable.class));
    }
}
