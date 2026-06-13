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
    private SegmentTopologyRuntimeAccess<Integer, String> topologyRuntime;

    @Mock
    private SegmentIndexTrackedOperationRunner<Integer, String> trackedRunner;

    @Test
    void maintenanceCommandsRunThroughTrackingAndInvalidateIterators() {
        enablePassThroughTracking();
        final SegmentIndexMaintenance adapter =
                new SegmentIndexMaintenanceSessionAdapter<>(delegate,
                        topologyRuntime, trackedRunner);

        assertDoesNotThrow(() -> {
            adapter.compact();
            adapter.compactAndWait();
            adapter.flush();
            adapter.flushAndWait();
            adapter.checkAndRepairConsistency();
        });

        verify(trackedRunner, times(5)).runTrackedVoid(any(Runnable.class));
        final InOrder inOrder = inOrder(delegate, topologyRuntime);
        inOrder.verify(delegate).compact();
        inOrder.verify(topologyRuntime).invalidateSegmentIterators();
        inOrder.verify(delegate).compactAndWait();
        inOrder.verify(topologyRuntime).invalidateSegmentIterators();
        inOrder.verify(delegate).flush();
        inOrder.verify(topologyRuntime).invalidateSegmentIterators();
        inOrder.verify(delegate).flushAndWait();
        inOrder.verify(topologyRuntime).invalidateSegmentIterators();
        inOrder.verify(delegate).checkAndRepairConsistency();
        inOrder.verify(topologyRuntime).invalidateSegmentIterators();
        verifyNoMoreInteractions(delegate, topologyRuntime, trackedRunner);
    }

    @Test
    void maintenanceFailureDoesNotInvalidateIterators() {
        enablePassThroughTracking();
        doThrow(new IllegalStateException("flush failed")).when(delegate)
                .flush();
        final SegmentIndexMaintenance adapter =
                new SegmentIndexMaintenanceSessionAdapter<>(delegate,
                        topologyRuntime, trackedRunner);

        assertThrows(IllegalStateException.class, adapter::flush);

        verify(trackedRunner).runTrackedVoid(any(Runnable.class));
        verify(delegate).flush();
        verify(topologyRuntime, never()).invalidateSegmentIterators();
        verifyNoMoreInteractions(delegate, topologyRuntime, trackedRunner);
    }

    @Test
    void constructorRejectsMissingCollaborators() {
        assertDoesNotThrow(() -> new SegmentIndexMaintenanceSessionAdapter<>(
                delegate, topologyRuntime, trackedRunner));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentIndexMaintenanceSessionAdapter<>(null,
                        topologyRuntime, trackedRunner));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentIndexMaintenanceSessionAdapter<>(delegate,
                        null, trackedRunner));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentIndexMaintenanceSessionAdapter<>(delegate,
                        topologyRuntime, null));
    }

    private void enablePassThroughTracking() {
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(trackedRunner).runTrackedVoid(any(Runnable.class));
    }
}
