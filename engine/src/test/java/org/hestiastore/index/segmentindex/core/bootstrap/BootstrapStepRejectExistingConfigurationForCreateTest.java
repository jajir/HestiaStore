package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.configuration;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.request;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.saveConfiguration;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class BootstrapStepRejectExistingConfigurationForCreateTest {

    private final BootstrapStepRejectExistingConfigurationForCreate<Integer, String> step =
            new BootstrapStepRejectExistingConfigurationForCreate<>();

    @Test
    void apply_allowsCreateWhenConfigurationIsMissing() {
        final MemDirectory directory = new MemDirectory();

        assertDoesNotThrow(() -> step.apply(
                request(directory, SegmentIndexBootstrapMode.CREATE),
                new SegmentIndexBootstrapState<>()));
    }

    @Test
    void apply_rejectsCreateWhenConfigurationExists() {
        final MemDirectory directory = new MemDirectory();
        saveConfiguration(directory,
                configuration("bootstrap-step-reject-create"));

        assertThrows(IndexException.class, () -> step.apply(
                request(directory, SegmentIndexBootstrapMode.CREATE),
                new SegmentIndexBootstrapState<>()));
    }

    @Test
    void apply_allowsOpenAndTryOpenWhenConfigurationExists() {
        final MemDirectory directory = new MemDirectory();
        saveConfiguration(directory,
                configuration("bootstrap-step-reject-open"));

        assertDoesNotThrow(() -> step.apply(
                request(directory, SegmentIndexBootstrapMode.OPEN),
                new SegmentIndexBootstrapState<>()));
        assertDoesNotThrow(() -> step.apply(
                request(directory, SegmentIndexBootstrapMode.TRY_OPEN),
                new SegmentIndexBootstrapState<>()));
    }
}
