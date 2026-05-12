package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.effectiveConfiguration;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.request;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.stateWithConfiguration;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.junit.jupiter.api.Test;

class BootstrapStepCreateExecutorRegistryTest {

    private final BootstrapStepCreateExecutorRegistry<Integer, String> step =
            new BootstrapStepCreateExecutorRegistry<>();

    @Test
    void apply_createsExecutorRegistryFromConfiguration() {
        final SegmentIndexBootstrapState<Integer, String> state =
                stateWithConfiguration(
                        effectiveConfiguration("bootstrap-step-executor"));

        assertDoesNotThrow(() -> step.apply(
                request(new MemDirectory(), SegmentIndexBootstrapMode.CREATE),
                state));

        assertFalse(state.getExecutorRegistry().wasClosed());
        step.closeResource();
        assertTrue(state.getExecutorRegistry().wasClosed());
    }

    @Test
    void closeResource_isNoOpBeforeApply() {
        assertDoesNotThrow(step::closeResource);
    }

    @Test
    void closeResource_doesNotCloseRegistryTwice() {
        final SegmentIndexBootstrapState<Integer, String> state =
                stateWithConfiguration(effectiveConfiguration(
                        "bootstrap-step-executor-close"));
        step.apply(request(new MemDirectory(), SegmentIndexBootstrapMode.CREATE),
                state);
        final ExecutorRegistry registry = state.getExecutorRegistry();

        assertDoesNotThrow(step::closeResource);
        assertDoesNotThrow(step::closeResource);

        assertTrue(registry.wasClosed());
    }
}
