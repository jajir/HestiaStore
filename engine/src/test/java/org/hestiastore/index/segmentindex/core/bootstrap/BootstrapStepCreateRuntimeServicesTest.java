package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.closeRuntimePreparationResources;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.configurationWithWal;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.effectiveConfiguration;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.executorRegistry;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.request;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.stateWithRuntimeInputs;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionResources;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BootstrapStepCreateRuntimeServicesTest {

    private SegmentIndexSessionResources<Integer, String> sessionResources;
    private ExecutorRegistry executorRegistry;
    private SegmentIndexBootstrapState<Integer, String> state;
    private BootstrapStepCreateRuntimeServices<Integer, String> step;
    private MemDirectory directory;

    @BeforeEach
    void setUp() {
        sessionResources = new SegmentIndexSessionResources<>();
        sessionResources.createSessionInfrastructure();
        step = new BootstrapStepCreateRuntimeServices<>(sessionResources);
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
    void constructor_rejectsNullSessionResources() {
        assertThrows(IllegalArgumentException.class,
                () -> new BootstrapStepCreateRuntimeServices<Integer, String>(
                        null));
    }

    @Test
    void apply_populatesRuntimeServicesInState() {
        prepareRuntimeInputs("bootstrap-step-services");

        step.apply(request(directory, SegmentIndexBootstrapMode.CREATE), state);

        assertNotNull(state.getRuntimeServices());
    }

    @Test
    void closeResource_keepsEarlierRuntimeResourcesOpenOnRollback() {
        prepareRuntimeInputs("bootstrap-step-services-rollback");
        step.apply(request(directory, SegmentIndexBootstrapMode.CREATE), state);

        step.closeResource();

        assertDoesNotThrow(
                () -> state.getRuntimeWalRuntime().appendPut(1, "one"));
    }

    @Test
    void closeResource_keepsEarlierRuntimeResourcesOpenAfterRuntimeWasCreated() {
        prepareRuntimeInputs("bootstrap-step-services-runtime-created");
        step.apply(request(directory, SegmentIndexBootstrapMode.CREATE), state);
        state.markIndexRuntimeCreated();

        step.closeResource();

        assertDoesNotThrow(
                () -> state.getRuntimeWalRuntime().appendPut(1, "one"));
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
