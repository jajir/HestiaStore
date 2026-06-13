package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.operations.SegmentIndexOperationAccess;
import org.hestiastore.index.segmentindex.core.segmentlease.SegmentLeaseService;
import org.hestiastore.index.segmentindex.core.session.SegmentTopologyRuntimeAccess;
import org.hestiastore.index.segmentindex.core.split.SplitService;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BootstrapRuntimeStateTest {

    @Mock
    private SegmentLeaseService<Integer, String> segmentLeaseService;

    @Mock
    private SplitService splitService;

    @Mock
    private SegmentTopologyRuntimeAccess<Integer, String> topologyRuntime;

    @Mock
    private WalRuntime<Integer, String> walRuntime;

    @Mock
    private MaintenanceService maintenanceService;

    @Mock
    private SegmentIndexOperationAccess<Integer, String> operationAccess;

    @Mock
    private RuntimeTuning runtimeTuning;

    @Mock
    private IndexRuntimeMonitoring runtimeMonitoring;

    private BootstrapRuntimeState<Integer, String> state;

    @BeforeEach
    void setUp() {
        state = new BootstrapRuntimeState<>();
    }

    @Test
    void storesRuntimeCollaborators() {
        final BootstrapWalCheckpoint maintenanceCheckpoint =
                new BootstrapWalCheckpoint();

        state.setSegmentLeaseService(segmentLeaseService);
        state.setSplitService(splitService);
        state.setTopologyRuntime(topologyRuntime);
        state.setWalRuntime(walRuntime);
        state.setMaintenanceCheckpoint(maintenanceCheckpoint);
        state.setMaintenanceService(maintenanceService);
        state.setOperationAccess(operationAccess);
        state.setRuntimeTuning(runtimeTuning);
        state.setRuntimeMonitoring(runtimeMonitoring);

        assertTrue(state.hasSplitService());
        assertTrue(state.hasWalRuntime());
        assertSame(segmentLeaseService, state.getSegmentLeaseService());
        assertSame(splitService, state.getSplitService());
        assertSame(topologyRuntime, state.getTopologyRuntime());
        assertSame(walRuntime, state.getWalRuntime());
        assertSame(maintenanceCheckpoint, state.getMaintenanceCheckpoint());
        assertSame(maintenanceService, state.getMaintenanceService());
        assertSame(operationAccess, state.getOperationAccess());
        assertSame(runtimeTuning, state.getRuntimeTuning());
        assertSame(runtimeMonitoring, state.getRuntimeMonitoring());
    }

    @Test
    void tracksRuntimeCloseOwnershipTransfer() {
        assertFalse(state.closeOwnershipTransferred());

        state.markCloseOwnershipTransferred();

        assertTrue(state.closeOwnershipTransferred());
    }
}
