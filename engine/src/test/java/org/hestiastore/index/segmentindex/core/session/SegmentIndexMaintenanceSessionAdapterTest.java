package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.hestiastore.index.segmentindex.maintenance.SegmentIndexMaintenance;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
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
    void maintenanceCommandsRunThroughTrackingAndInvalidateIterators() {
        enablePassThroughTracking();
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

        verify(trackedRunner, times(5)).runTrackedVoid(any(Runnable.class));
        final InOrder inOrder = inOrder(delegate, sessionOwner);
        inOrder.verify(delegate).compact();
        inOrder.verify(sessionOwner).invalidateSegmentIterators();
        inOrder.verify(delegate).compactAndWait();
        inOrder.verify(sessionOwner).invalidateSegmentIterators();
        inOrder.verify(delegate).flush();
        inOrder.verify(sessionOwner).invalidateSegmentIterators();
        inOrder.verify(delegate).flushAndWait();
        inOrder.verify(sessionOwner).invalidateSegmentIterators();
        inOrder.verify(delegate).checkAndRepairConsistency();
        inOrder.verify(sessionOwner).invalidateSegmentIterators();
        verifyNoMoreInteractions(delegate, sessionOwner, trackedRunner);
    }

    @Test
    void maintenanceFailureDoesNotInvalidateIterators() {
        enablePassThroughTracking();
        doThrow(new IllegalStateException("flush failed")).when(delegate)
                .flush();
        final SegmentIndexMaintenance adapter =
                new SegmentIndexMaintenanceSessionAdapter<>(delegate,
                        sessionOwner, trackedRunner);

        assertThrows(IllegalStateException.class, adapter::flush);

        verify(trackedRunner).runTrackedVoid(any(Runnable.class));
        verify(delegate).flush();
        verify(sessionOwner, never()).invalidateSegmentIterators();
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

    private void enablePassThroughTracking() {
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(trackedRunner).runTrackedVoid(any(Runnable.class));
    }
}
