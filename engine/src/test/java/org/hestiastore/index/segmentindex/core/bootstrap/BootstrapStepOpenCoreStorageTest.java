package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.closeRuntimePreparationResources;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.effectiveConfiguration;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.executorRegistry;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.request;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.stateWithRuntimeInputs;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

        step.apply(request(new MemDirectory(), SegmentIndexBootstrapMode.CREATE),
                state);

        assertNotNull(state.getCoreStorage());
        assertNotNull(state.getCoreStorage().keyToSegmentMap());
        assertNotNull(state.getCoreStorage().segmentRegistry());
    }

    @Test
    void closeResource_closesCoreStorageOnRollback() {
        prepareState("bootstrap-step-core-storage-rollback");
        step.apply(request(new MemDirectory(), SegmentIndexBootstrapMode.CREATE),
                state);

        step.closeResource();

        assertTrue(state.getCoreStorage().keyToSegmentMap().wasClosed());
    }

    @Test
    void closeResource_skipsCleanupAfterRuntimeWasCreated() {
        prepareState("bootstrap-step-core-storage-runtime-created");
        step.apply(request(new MemDirectory(), SegmentIndexBootstrapMode.CREATE),
                state);
        state.markIndexRuntimeCreated();

        step.closeResource();

        assertFalse(state.getCoreStorage().keyToSegmentMap()
                .wasClosed());
    }

    private void prepareState(final String indexName) {
        final EffectiveIndexConfiguration<Integer, String> configuration =
                effectiveConfiguration(indexName);
        executorRegistry = executorRegistry(configuration);
        state = stateWithRuntimeInputs(configuration, executorRegistry);
    }
}
