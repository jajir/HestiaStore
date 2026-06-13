package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.closeRuntimePreparationResources;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.configurationWithWal;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.effectiveConfiguration;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.executorRegistry;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.request;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.stateWithRuntimeInputs;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionInfrastructure;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionResources;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BootstrapRuntimeServiceStepsTest {

    private SegmentIndexSessionResources<Integer, String> sessionResources;
    private ExecutorRegistry executorRegistry;
    private SegmentIndexBootstrapState<Integer, String> state;
    private MemDirectory directory;

    @BeforeEach
    void setUp() {
        sessionResources = new SegmentIndexSessionResources<>();
        sessionResources.setSessionInfrastructure(
                SegmentIndexSessionInfrastructure.create());
        directory = new MemDirectory();
    }

    @AfterEach
    void tearDown() {
        if (state != null) {
            closeRuntimePreparationResources(state);
        }
        if (executorRegistry != null && !executorRegistry.wasClosed()) {
            executorRegistry.close();
        }
    }

    @Test
    void constructors_rejectMissingSessionResources() {
        assertAll(
                () -> assertThrows(IllegalArgumentException.class,
                        () -> new BootstrapStepCreateMaintenance<Integer, String>(
                                null)),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> new BootstrapStepInitializeWal<Integer, String>(
                                null)),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> new BootstrapStepCreateRuntimeMonitoring<Integer, String>(
                                null)),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> new BootstrapStepCreateOperationAccess<Integer, String>(
                                null)));
    }

    @Test
    void steps_populateRuntimeCollaboratorsInState() {
        prepareRuntimeInputs("bootstrap-runtime-service-steps");

        applyRuntimeServiceSteps();

        assertNotNull(state.getRuntimeMaintenanceService());
        assertNotNull(state.getRuntimeMaintenanceCheckpoint());
        assertNotNull(state.getRuntimeMonitoring());
        assertNotNull(state.getRuntimeTuning());
        assertNotNull(state.getRuntimeOperationAccess());
    }

    @Test
    void initializeWal_bindsMaintenanceCheckpointToStorage() {
        prepareRuntimeInputs("bootstrap-runtime-service-steps-wal");

        applyRuntimeServiceSteps();

        assertDoesNotThrow(
                () -> state.getRuntimeMaintenanceCheckpoint().checkpoint());
    }

    private void applyRuntimeServiceSteps() {
        final SegmentIndexBootstrapRequest<Integer, String> request =
                request(directory, SegmentIndexBootstrapMode.CREATE);
        new BootstrapStepCreateMaintenance<>(sessionResources).apply(request,
                state);
        new BootstrapStepInitializeWal<>(sessionResources).apply(request,
                state);
        new BootstrapStepCreateRuntimeMonitoring<>(sessionResources).apply(
                request, state);
        new BootstrapStepCreateRuntimeTuning<Integer, String>().apply(request,
                state);
        new BootstrapStepCreateOperationAccess<>(sessionResources).apply(
                request, state);
    }

    private void prepareRuntimeInputs(final String indexName) {
        final EffectiveIndexConfiguration<Integer, String> configuration =
                effectiveConfiguration(configurationWithWal(indexName));
        executorRegistry = executorRegistry(configuration);
        state = stateWithRuntimeInputs(configuration, executorRegistry);
        final SegmentIndexBootstrapRequest<Integer, String> request =
                request(directory, SegmentIndexBootstrapMode.CREATE);
        new BootstrapStepOpenKeyToSegmentMap<Integer, String>().apply(request,
                state);
        new BootstrapStepCreateChunkStoreCache<Integer, String>().apply(
                request, state);
        new BootstrapStepOpenSegmentRegistry<Integer, String>().apply(request,
                state);
        new BootstrapStepOpenCoreStorage<Integer, String>().apply(request,
                state);
        new BootstrapStepCreateRuntimeTopology<>(sessionResources).apply(
                request, state);
        new BootstrapStepOpenRuntimeWal<Integer, String>().apply(
                request, state);
    }
}
