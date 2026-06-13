package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.effectiveConfiguration;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.request;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.stateWithConfiguration;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.operations.SegmentIndexOperationAccess;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionResources;
import org.hestiastore.index.segmentindex.core.session.SegmentTopologyRuntimeAccess;
import org.hestiastore.index.segmentindex.core.storage.CoreStorageRuntime;
import org.hestiastore.index.segmentindex.core.storage.StorageService;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BootstrapStepCompleteStartupTest {

    @Mock
    private SegmentIndexSessionResources<Integer, String> sessionResources;
    @Mock
    private StorageService<Integer, String> storageService;
    @Mock
    private SegmentTopologyRuntimeAccess<Integer, String> topologyRuntime;

    private BootstrapStepCompleteStartup<Integer, String> step;

    @BeforeEach
    void setUp() {
        step = new BootstrapStepCompleteStartup<>(sessionResources);
    }

    @Test
    void constructor_rejectsNullSessionResources() {
        assertThrows(IllegalArgumentException.class,
                () -> new BootstrapStepCompleteStartup<Integer, String>(null));
    }

    @Test
    void apply_completesStartupWithoutConsistencyCheck() {
        final SegmentIndexBootstrapState<Integer, String> state =
                stateWithRuntimeAccess("bootstrap-step-complete-startup");

        assertDoesNotThrow(() -> step.apply(
                request(new MemDirectory(), SegmentIndexBootstrapMode.OPEN),
                state));

        final InOrder inOrder = inOrder(storageService, sessionResources,
                topologyRuntime);
        inOrder.verify(storageService).recoverFromWal(any());
        inOrder.verify(storageService).cleanupOrphanedSegmentDirectories();
        inOrder.verify(sessionResources).markReady();
        inOrder.verify(sessionResources).wasStaleLockRecovered();
        inOrder.verify(topologyRuntime).requestFullSplitScan();
        verifyNoMoreInteractions(storageService, sessionResources,
                topologyRuntime);
    }

    @Test
    void apply_runsConsistencyCheckAfterStaleLockRecovery() {
        final SegmentIndexBootstrapState<Integer, String> state =
                stateWithRuntimeAccess("bootstrap-step-complete-startup");
        when(sessionResources.wasStaleLockRecovered()).thenReturn(true);

        assertDoesNotThrow(() -> step.apply(
                request(new MemDirectory(), SegmentIndexBootstrapMode.OPEN),
                state));

        final InOrder inOrder = inOrder(storageService, sessionResources,
                topologyRuntime);
        inOrder.verify(storageService).recoverFromWal(any());
        inOrder.verify(storageService).cleanupOrphanedSegmentDirectories();
        inOrder.verify(sessionResources).markReady();
        inOrder.verify(sessionResources).wasStaleLockRecovered();
        inOrder.verify(storageService).runStartupConsistencyCheck();
        inOrder.verify(topologyRuntime, times(2)).requestFullSplitScan();
        verifyNoMoreInteractions(storageService, sessionResources,
                topologyRuntime);
    }

    @SuppressWarnings("unchecked")
    private SegmentIndexBootstrapState<Integer, String> stateWithRuntimeAccess(
            final String indexName) {
        final SegmentIndexBootstrapState<Integer, String> state =
                stateWithConfiguration(effectiveConfiguration(indexName));
        state.setCoreStorageRuntime(new CoreStorageRuntime<>(
                mock(RuntimeTuningState.class),
                storageService,
                mock(SegmentRegistry.class),
                mock(KeyToSegmentMap.class)));
        state.setRuntimeTopologyRuntime(topologyRuntime);
        state.setRuntimeOperationAccess(
                mock(SegmentIndexOperationAccess.class));
        state.setRuntimeMaintenanceService(mock(MaintenanceService.class));
        state.setRuntimeMonitoring(mock(IndexRuntimeMonitoring.class));
        state.setRuntimeTuning(mock(RuntimeTuning.class));
        return state;
    }
}
