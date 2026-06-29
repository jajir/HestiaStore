package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.hestiastore.index.segmentindex.core.execution.MappedSegmentMaintenanceService;
import org.hestiastore.index.segmentindex.core.execution.SegmentIteratorService;
import org.hestiastore.index.segmentindex.core.split.SplitRuntime;
import org.hestiastore.index.segmentindex.core.storage.StorageCoordinator;
import org.hestiastore.index.segmentindex.SegmentIndexMaintenance;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MaintenanceApiAdapterTest {

    @Mock
    private MappedSegmentMaintenanceService<Integer, String> maintenanceService;

    @Mock
    private StorageCoordinator<Integer, String> storageService;

    @Mock
    private SplitRuntime<Integer, String> splitService;

    @Mock
    private SegmentIteratorService<Integer, String> streamingService;

    private SegmentIndexStateMachine stateMachine;
    private SessionOperationGate operationGate;

    @BeforeEach
    void setUp() {
        stateMachine = new SegmentIndexStateMachine();
        stateMachine.markReady();
        operationGate = SessionOperationGate.create();
    }

    @Test
    void maintenanceCommandsRunThroughTrackingAndInvalidateIterators() {
        final SegmentIndexMaintenance adapter =
                new MaintenanceApiAdapter<>(
                        maintenanceService, storageService, splitService,
                        streamingService, stateMachine, operationGate);

        assertDoesNotThrow(() -> {
            adapter.compact();
            adapter.compactAndWait();
            adapter.flush();
            adapter.flushAndWait();
            adapter.checkAndRepairConsistency();
        });

        final InOrder inOrder = inOrder(maintenanceService, storageService,
                splitService, streamingService);
        inOrder.verify(maintenanceService).compact();
        inOrder.verify(streamingService).invalidateIterators();
        inOrder.verify(maintenanceService).compactAndWait();
        inOrder.verify(streamingService).invalidateIterators();
        inOrder.verify(maintenanceService).flush();
        inOrder.verify(streamingService).invalidateIterators();
        inOrder.verify(maintenanceService).flushAndWait();
        inOrder.verify(streamingService).invalidateIterators();
        inOrder.verify(storageService).checkAndRepairConsistency();
        inOrder.verify(splitService).requestFullSplitScan();
        inOrder.verify(streamingService).invalidateIterators();
        verifyNoMoreInteractions(maintenanceService, storageService,
                splitService, streamingService);
    }

    @Test
    void maintenanceFailureDoesNotInvalidateIterators() {
        doThrow(new IllegalStateException("flush failed")).when(maintenanceService)
                .flush();
        final SegmentIndexMaintenance adapter =
                new MaintenanceApiAdapter<>(
                        maintenanceService, storageService, splitService,
                        streamingService, stateMachine, operationGate);

        assertThrows(IllegalStateException.class, adapter::flush);

        verify(maintenanceService).flush();
        verify(streamingService, never()).invalidateIterators();
        assertDoesNotThrow(operationGate::awaitOperationDrain);
        verifyNoMoreInteractions(maintenanceService, storageService,
                splitService, streamingService);
    }

    @Test
    void constructorRejectsMissingCollaborators() {
        assertDoesNotThrow(() -> new MaintenanceApiAdapter<>(
                maintenanceService, storageService, splitService,
                streamingService, stateMachine, operationGate));
        assertThrows(IllegalArgumentException.class,
                () -> new MaintenanceApiAdapter<>(null,
                        storageService, splitService, streamingService,
                        stateMachine, operationGate));
        assertThrows(IllegalArgumentException.class,
                () -> new MaintenanceApiAdapter<>(
                        maintenanceService, null, splitService,
                        streamingService, stateMachine, operationGate));
        assertThrows(IllegalArgumentException.class,
                () -> new MaintenanceApiAdapter<>(
                        maintenanceService, storageService, null,
                        streamingService, stateMachine, operationGate));
        assertThrows(IllegalArgumentException.class,
                () -> new MaintenanceApiAdapter<>(
                        maintenanceService, storageService, splitService, null,
                        stateMachine, operationGate));
        assertThrows(IllegalArgumentException.class,
                () -> new MaintenanceApiAdapter<>(
                        maintenanceService, storageService, splitService,
                        streamingService, null, operationGate));
        assertThrows(IllegalArgumentException.class,
                () -> new MaintenanceApiAdapter<>(
                        maintenanceService, storageService, splitService,
                        streamingService, stateMachine, null));
    }
}
