package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.effectiveConfiguration;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.executorRegistry;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.request;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.stateWithRuntimeInputs;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.chunkstorecache.ChunkStoreCache;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.operations.SegmentIndexOperationAccess;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexRuntimeServices;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionResources;
import org.hestiastore.index.segmentindex.core.session.SegmentTopologyRuntimeAccess;
import org.hestiastore.index.segmentindex.core.storage.CoreStorageRuntime;
import org.hestiastore.index.segmentindex.core.storage.StorageService;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BootstrapStepCreateRuntimeTest {

    private SegmentIndexSessionResources<Integer, String> sessionResources;
    private ExecutorRegistry executorRegistry;
    private BootstrapStepCreateRuntime<Integer, String> step;
    private SegmentIndexBootstrapState<Integer, String> state;
    private SegmentTopologyRuntimeAccess<Integer, String> topologyRuntime;
    private KeyToSegmentMap<Integer> keyToSegmentMap;
    private SegmentRegistry<Integer, String> segmentRegistry;
    private StorageService<Integer, String> storageService;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        sessionResources = new SegmentIndexSessionResources<>();
        sessionResources.createSessionInfrastructure();
        step = new BootstrapStepCreateRuntime<>(sessionResources);
        topologyRuntime = mock(SegmentTopologyRuntimeAccess.class);
        keyToSegmentMap = mock(KeyToSegmentMap.class);
        segmentRegistry = mock(SegmentRegistry.class);
        storageService = mock(StorageService.class);
    }

    @AfterEach
    void tearDown() {
        if (executorRegistry != null && !executorRegistry.wasClosed()) {
            executorRegistry.close();
        }
    }

    @Test
    void constructor_rejectsNullSessionResources() {
        assertThrows(IllegalArgumentException.class,
                () -> new BootstrapStepCreateRuntime<Integer, String>(null));
    }

    @Test
    void apply_createsRuntimeAndStoresItInSessionResources() {
        prepareRuntimeState("bootstrap-step-runtime");

        assertDoesNotThrow(() -> step.apply(
                request(new MemDirectory(), SegmentIndexBootstrapMode.CREATE),
                state));

        assertTrue(state.indexRuntimeWasCreated());
    }

    @Test
    void closeResource_closesRuntimeAfterFailedInitialization() {
        prepareRuntimeState("bootstrap-step-runtime-rollback");
        step.apply(request(new MemDirectory(), SegmentIndexBootstrapMode.CREATE),
                state);
        when(keyToSegmentMap.wasClosed()).thenReturn(false);

        step.closeResource();

        verify(topologyRuntime).closeSplitRuntime();
        verify(segmentRegistry).close();
        verify(keyToSegmentMap).close();
        verify(storageService).closeWal();
    }

    @Test
    void closeResource_isNoopBeforeRuntimeWasCreated() {
        assertDoesNotThrow(step::closeResource);
    }

    @SuppressWarnings("unchecked")
    private void prepareRuntimeState(final String indexName) {
        final EffectiveIndexConfiguration<Integer, String> configuration =
                effectiveConfiguration(indexName);
        executorRegistry = executorRegistry(configuration);
        state = stateWithRuntimeInputs(configuration, executorRegistry);
        state.setKeyToSegmentMap(keyToSegmentMap);
        state.setChunkStoreCache(mock(ChunkStoreCache.class));
        state.setSegmentRegistry(segmentRegistry);
        state.setCoreStorageRuntime(new CoreStorageRuntime<>(
                mock(RuntimeTuningState.class), storageService));
        state.setRuntimeTopologyRuntime(topologyRuntime);
        state.setRuntimeServices(new SegmentIndexRuntimeServices<>(
                mock(SegmentIndexOperationAccess.class),
                mock(MaintenanceService.class),
                mock(IndexRuntimeMonitoring.class),
                mock(RuntimeTuning.class)));
    }
}
