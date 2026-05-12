package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.CONFIGURATION_FILE_NAME;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.effectiveConfiguration;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.request;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.stateWithConfiguration;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.configuration.persistence.IndexConfigurationStorage;
import org.junit.jupiter.api.Test;

class BootstrapStepWriteConfigurationTest {

    private final BootstrapStepWriteConfiguration<Integer, String> step =
            new BootstrapStepWriteConfiguration<>();

    @Test
    void apply_savesConfigurationWhenWriteRequired() {
        final MemDirectory directory = new MemDirectory();
        final SegmentIndexBootstrapState<Integer, String> state =
                stateWithConfiguration(
                        effectiveConfiguration("bootstrap-step-write"));
        state.setConfigurationWriteRequired(true);

        assertDoesNotThrow(() -> step.apply(
                request(directory, SegmentIndexBootstrapMode.CREATE), state));

        assertTrue(directory.isFileExists(CONFIGURATION_FILE_NAME));
        assertEquals("bootstrap-step-write",
                new IndexConfigurationStorage<Integer, String>(directory)
                        .load().identity().name());
    }

    @Test
    void apply_skipsSaveWhenWriteIsNotRequired() {
        final MemDirectory directory = new MemDirectory();
        final SegmentIndexBootstrapState<Integer, String> state =
                stateWithConfiguration(effectiveConfiguration(
                        "bootstrap-step-write-skipped"));
        state.setConfigurationWriteRequired(false);

        assertDoesNotThrow(() -> step.apply(
                request(directory, SegmentIndexBootstrapMode.OPEN), state));

        assertFalse(directory.isFileExists(CONFIGURATION_FILE_NAME));
    }
}
