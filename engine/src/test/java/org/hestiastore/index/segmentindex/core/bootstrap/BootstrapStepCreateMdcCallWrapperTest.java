package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.configuration;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.effectiveConfiguration;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.request;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.stateWithConfiguration;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class BootstrapStepCreateMdcCallWrapperTest {

    private static final String MDC_INDEX_NAME_KEY = "index.name";

    private final BootstrapStepCreateMdcCallWrapper<Integer, String> step =
            new BootstrapStepCreateMdcCallWrapper<>();

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void apply_createsCallWrapperWhenContextLoggingIsEnabled() {
        final SegmentIndexBootstrapState<Integer, String> state =
                stateWithConfiguration(effectiveConfiguration(configuration(
                        "bootstrap-step-mdc-enabled", true)));

        assertDoesNotThrow(() -> step.apply(
                request(new MemDirectory(), SegmentIndexBootstrapMode.CREATE),
                state));

        assertTrue(state.hasIndexMdcCallWrapper());
        state.getIndexMdcCallWrapper().run(() -> assertEquals(
                "bootstrap-step-mdc-enabled",
                MDC.get(MDC_INDEX_NAME_KEY)));
        assertNull(MDC.get(MDC_INDEX_NAME_KEY));
    }

    @Test
    void apply_skipsCallWrapperWhenContextLoggingIsDisabled() {
        final SegmentIndexBootstrapState<Integer, String> state =
                stateWithConfiguration(effectiveConfiguration(configuration(
                        "bootstrap-step-mdc-disabled", false)));

        assertDoesNotThrow(() -> step.apply(
                request(new MemDirectory(), SegmentIndexBootstrapMode.CREATE),
                state));

        assertFalse(state.hasIndexMdcCallWrapper());
    }
}
