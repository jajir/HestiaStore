package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.effectiveConfiguration;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.executorRegistry;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.request;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.stateWithRuntimeInputs;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BootstrapStepOpenSegmentRegistryTest {

    private MemDirectory directory;
    private ExecutorRegistry executorRegistry;
    private SegmentIndexBootstrapState<Integer, String> state;
    private BootstrapStepOpenSegmentRegistry<Integer, String> step;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        step = new BootstrapStepOpenSegmentRegistry<>();
    }

    @AfterEach
    void tearDown() {
        if (state != null) {
            closeSegmentRegistryIfOpened();
        }
        if (executorRegistry != null && !executorRegistry.wasClosed()) {
            executorRegistry.close();
        }
    }

    @Test
    void apply_populatesSegmentRegistryInState() {
        prepareState("bootstrap-step-segment-registry");

        step.apply(request(directory, SegmentIndexBootstrapMode.CREATE),
                state);

        assertNotNull(state.getSegmentRegistry());
    }

    @Test
    void closeResource_closesSegmentRegistryOnRollback() {
        prepareState("bootstrap-step-segment-registry-rollback");
        step.apply(request(directory, SegmentIndexBootstrapMode.CREATE),
                state);

        step.closeResource();

        assertThrows(RuntimeException.class,
                () -> state.getSegmentRegistry().createSegment());
    }

    private void prepareState(final String indexName) {
        final EffectiveIndexConfiguration<Integer, String> configuration =
                effectiveConfiguration(indexName);
        executorRegistry = executorRegistry(configuration);
        state = stateWithRuntimeInputs(configuration, executorRegistry);
        new BootstrapStepCreateChunkStoreCache<Integer, String>().apply(
                request(directory, SegmentIndexBootstrapMode.CREATE), state);
    }

    private void closeSegmentRegistryIfOpened() {
        try {
            final SegmentRegistry<Integer, String> segmentRegistry =
                    state.getSegmentRegistry();
            segmentRegistry.close();
        } catch (final RuntimeException ignored) {
            // Best-effort test cleanup.
        }
    }
}
