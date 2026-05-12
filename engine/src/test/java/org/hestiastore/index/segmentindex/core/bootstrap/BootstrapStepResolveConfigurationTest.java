package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.configuration;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.request;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.saveConfiguration;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class BootstrapStepResolveConfigurationTest {

    private final BootstrapStepResolveConfiguration<Integer, String> step =
            new BootstrapStepResolveConfiguration<>();

    @Test
    void apply_resolvesCreateConfigurationAndMarksWriteRequired() {
        final MemDirectory directory = new MemDirectory();
        final SegmentIndexBootstrapState<Integer, String> state =
                new SegmentIndexBootstrapState<>();

        assertDoesNotThrow(() -> step.apply(request(directory,
                configuration("bootstrap-step-resolve-create"),
                SegmentIndexBootstrapMode.CREATE), state));

        assertEquals("bootstrap-step-resolve-create",
                state.getConfiguration().identity().name());
        assertTrue(state.isConfigurationWriteRequired());
    }

    @Test
    void apply_resolvesOpenConfigurationAndMarksCleanWhenUnchanged() {
        final MemDirectory directory = new MemDirectory();
        saveConfiguration(directory,
                configuration("bootstrap-step-resolve-open", false, 1));
        final SegmentIndexBootstrapState<Integer, String> state =
                new SegmentIndexBootstrapState<>();

        assertDoesNotThrow(() -> step.apply(request(directory,
                configuration("bootstrap-step-resolve-open", false, 1),
                SegmentIndexBootstrapMode.OPEN), state));

        assertEquals("bootstrap-step-resolve-open",
                state.getConfiguration().identity().name());
        assertFalse(state.isConfigurationWriteRequired());
    }

    @Test
    void apply_resolvesTryOpenLikeOpenAndMarksWriteRequiredWhenOverridesChange() {
        final MemDirectory directory = new MemDirectory();
        saveConfiguration(directory,
                configuration("bootstrap-step-resolve-try-open", false, 1));
        final SegmentIndexBootstrapState<Integer, String> state =
                new SegmentIndexBootstrapState<>();

        assertDoesNotThrow(() -> step.apply(request(directory,
                configuration("bootstrap-step-resolve-try-open", false, 2),
                SegmentIndexBootstrapMode.TRY_OPEN), state));

        assertEquals(2, state.getConfiguration().maintenance()
                .registryLifecycleThreads());
        assertTrue(state.isConfigurationWriteRequired());
    }
}
