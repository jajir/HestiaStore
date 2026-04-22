package org.hestiastore.index.segmentindex.core.maintenance;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.routing.IndexOperationTrackingAccess;
import org.hestiastore.index.segmentindex.core.routing.SegmentIndexTrackedOperationRunner;
import org.hestiastore.index.segmentindex.core.storage.IndexConsistencyCoordinator;
import org.hestiastore.index.segmentindex.core.session.state.IndexState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SegmentIndexMaintenanceCommandsTest {

    private SegmentIndexMaintenanceAccess<Integer, String> maintenanceAccess;
    private IndexConsistencyCoordinator<Integer, String> consistencyCoordinator;
    private SegmentIndexMaintenanceCommands<Integer, String> commands;

    @BeforeEach
    void setUp() {
        maintenanceAccess = mock(SegmentIndexMaintenanceAccess.class);
        consistencyCoordinator = mock(IndexConsistencyCoordinator.class);
        commands = new SegmentIndexMaintenanceCommands<>(
                new SegmentIndexTrackedOperationRunner<>(this::readyState,
                        IndexOperationTrackingAccess.create()),
                maintenanceAccess, consistencyCoordinator);
    }

    @Test
    void trackedMaintenanceCommandsDelegateToUnderlyingCoordinators() {
        commands.flush();
        commands.flushAndWait();
        commands.compact();
        commands.compactAndWait();
        commands.checkAndRepairConsistency();

        verify(maintenanceAccess).flush();
        verify(maintenanceAccess).flushAndWait();
        verify(maintenanceAccess).compact();
        verify(maintenanceAccess).compactAndWait();
        verify(consistencyCoordinator).checkAndRepairConsistency();
    }

    private IndexState<Integer, String> readyState() {
        return new IndexState<>() {
            @Override
            public SegmentIndexState state() {
                return SegmentIndexState.READY;
            }

            @Override
            public IndexState<Integer, String> onReady() {
                return this;
            }

            @Override
            public IndexState<Integer, String> onClose() {
                return this;
            }

            @Override
            public IndexState<Integer, String> finishClose() {
                return this;
            }

            @Override
            public void tryPerformOperation() {
            }
        };
    }
}
