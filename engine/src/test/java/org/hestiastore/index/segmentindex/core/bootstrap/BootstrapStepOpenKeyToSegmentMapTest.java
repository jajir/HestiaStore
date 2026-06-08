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

class BootstrapStepOpenKeyToSegmentMapTest {

    private MemDirectory directory;
    private ExecutorRegistry executorRegistry;
    private SegmentIndexBootstrapState<Integer, String> state;
    private BootstrapStepOpenKeyToSegmentMap<Integer, String> step;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        step = new BootstrapStepOpenKeyToSegmentMap<>();
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
    void apply_populatesKeyToSegmentMapInState() {
        prepareState("bootstrap-step-key-map");

        step.apply(request(directory, SegmentIndexBootstrapMode.CREATE),
                state);

        assertNotNull(state.getKeyToSegmentMap());
    }

    @Test
    void closeResource_closesKeyToSegmentMapOnRollback() {
        prepareState("bootstrap-step-key-map-rollback");
        step.apply(request(directory, SegmentIndexBootstrapMode.CREATE),
                state);

        step.closeResource();

        assertTrue(state.getKeyToSegmentMap().wasClosed());
    }

    @Test
    void closeResource_skipsCleanupAfterRuntimeWasCreated() {
        prepareState("bootstrap-step-key-map-runtime-created");
        step.apply(request(directory, SegmentIndexBootstrapMode.CREATE),
                state);
        state.markIndexRuntimeCreated();

        step.closeResource();

        assertFalse(state.getKeyToSegmentMap().wasClosed());
    }

    private void prepareState(final String indexName) {
        final EffectiveIndexConfiguration<Integer, String> configuration =
                effectiveConfiguration(indexName);
        executorRegistry = executorRegistry(configuration);
        state = stateWithRuntimeInputs(configuration, executorRegistry);
    }
}
