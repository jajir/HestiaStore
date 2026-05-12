package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.configuration;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.request;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.saveConfiguration;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class BootstrapStepCheckExistingConfigurationTest {

    private final BootstrapStepCheckExistingConfiguration<Integer, String> step =
            new BootstrapStepCheckExistingConfiguration<>();

    @Test
    void apply_setsNotFoundForTryOpenWhenConfigurationIsMissing() {
        final MemDirectory directory = new MemDirectory();
        final SegmentIndexBootstrapState<Integer, String> state =
                new SegmentIndexBootstrapState<>();

        assertDoesNotThrow(() -> step.apply(
                request(directory, SegmentIndexBootstrapMode.TRY_OPEN),
                state));

        assertTrue(state.hasResult());
        assertEquals(SegmentIndexBootstrapStatus.NOT_FOUND,
                state.getResult().status());
        assertTrue(state.getResult().index().isEmpty());
    }

    @Test
    void apply_keepsTryOpenFlowRunningWhenConfigurationExists() {
        final MemDirectory directory = new MemDirectory();
        saveConfiguration(directory,
                configuration("bootstrap-step-check-existing"));
        final SegmentIndexBootstrapState<Integer, String> state =
                new SegmentIndexBootstrapState<>();

        assertDoesNotThrow(() -> step.apply(
                request(directory, SegmentIndexBootstrapMode.TRY_OPEN),
                state));

        assertFalse(state.hasResult());
    }

    @Test
    void apply_rejectsCreateWhenConfigurationExists() {
        final MemDirectory directory = new MemDirectory();
        saveConfiguration(directory,
                configuration("bootstrap-step-check-create-existing"));
        final SegmentIndexBootstrapRequest<Integer, String> request =
                request(directory, SegmentIndexBootstrapMode.CREATE);

        assertThrows(IndexException.class,
                () -> step.apply(request, new SegmentIndexBootstrapState<>()));
    }

    @Test
    void apply_rejectsOpenWhenConfigurationIsMissing() {
        final MemDirectory directory = new MemDirectory();
        final SegmentIndexBootstrapRequest<Integer, String> request =
                request(directory, SegmentIndexBootstrapMode.OPEN);

        assertThrows(IndexException.class,
                () -> step.apply(request, new SegmentIndexBootstrapState<>()));
    }

    @Test
    void apply_allowsCreateWhenConfigurationIsMissing() {
        final MemDirectory directory = new MemDirectory();
        final SegmentIndexBootstrapState<Integer, String> state =
                new SegmentIndexBootstrapState<>();

        assertDoesNotThrow(() -> step.apply(
                request(directory, SegmentIndexBootstrapMode.CREATE),
                state));

        assertFalse(state.hasResult());
    }

    @Test
    void apply_allowsOpenWhenConfigurationExists() {
        final MemDirectory directory = new MemDirectory();
        saveConfiguration(directory,
                configuration("bootstrap-step-check-open-existing"));
        final SegmentIndexBootstrapState<Integer, String> state =
                new SegmentIndexBootstrapState<>();

        assertDoesNotThrow(() -> step.apply(
                request(directory, SegmentIndexBootstrapMode.OPEN),
                state));

        assertFalse(state.hasResult());
    }
}
