package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.closeRuntimePreparationResources;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.effectiveConfiguration;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.executorRegistry;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.request;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.stateWithRuntimeInputs;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BootstrapStepOpenCoreStorageTest {

    private ExecutorRegistry executorRegistry;
    private MemDirectory directory;
    private SegmentIndexBootstrapState<Integer, String> state;
    private BootstrapStepOpenCoreStorage<Integer, String> step;

    @BeforeEach
    void setUp() {
        step = new BootstrapStepOpenCoreStorage<>();
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
    void apply_populatesCoreStorageInState() {
        prepareState("bootstrap-step-core-storage");

        step.apply(request(directory, SegmentIndexBootstrapMode.CREATE),
                state);

        assertTrue(state.hasCoreStorage());
        assertNotNull(state.getRuntimeTuningState());
        assertNotNull(state.getStorageService());
    }

    @Test
    void closeResource_doesNotOwnEarlierStorageResources() {
        prepareState("bootstrap-step-core-storage-rollback");
        step.apply(request(directory, SegmentIndexBootstrapMode.CREATE),
                state);

        step.closeResource();

        assertTrue(state.hasCoreStorage());
    }

    private void prepareState(final String indexName) {
        final EffectiveIndexConfiguration<Integer, String> configuration =
                effectiveConfiguration(indexName);
        directory = new MemDirectory();
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
    }
}
