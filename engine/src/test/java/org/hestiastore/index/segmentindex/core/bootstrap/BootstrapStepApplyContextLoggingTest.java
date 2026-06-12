package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.configuration;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.effectiveConfiguration;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.request;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.when;

import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.configuration.user.IndexConfiguration;
import org.hestiastore.index.segmentindex.logging.SegmentIndexMdcLoggingAdapter;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionResource;
import org.hestiastore.index.segmentindex.maintenance.SegmentIndexMaintenance;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BootstrapStepApplyContextLoggingTest {

    @Mock
    private SegmentIndexSessionResource<Integer, String> indexHandle;

    @Mock
    private RuntimeTuning runtimeTuning;

    @Mock
    private IndexRuntimeMonitoring runtimeMonitoring;

    @Mock
    private SegmentIndexMaintenance maintenance;

    private BootstrapStepApplyContextLogging<Integer, String> step;

    @BeforeEach
    void setUp() {
        step = new BootstrapStepApplyContextLogging<>();
    }

    @Test
    void apply_keepsIndexHandleWhenContextLoggingDisabled() {
        final IndexConfiguration<Integer, String> configuration = configuration(
                "bootstrap-step-context-logging-disabled", false);
        final SegmentIndexBootstrapState<Integer, String> state =
                stateWithIndexHandle(configuration);

        assertDoesNotThrow(() -> step.apply(
                request(new MemDirectory(), configuration,
                        SegmentIndexBootstrapMode.CREATE),
                state));

        assertSame(indexHandle, state.getIndexHandle());
    }

    @Test
    void apply_wrapsIndexHandleWhenContextLoggingEnabled() {
        when(indexHandle.runtimeTuning()).thenReturn(runtimeTuning);
        when(indexHandle.runtimeMonitoring()).thenReturn(runtimeMonitoring);
        when(indexHandle.maintenance()).thenReturn(maintenance);
        final IndexConfiguration<Integer, String> configuration = configuration(
                "bootstrap-step-context-logging-enabled", true);
        final SegmentIndexBootstrapState<Integer, String> state =
                stateWithIndexHandle(configuration);

        assertDoesNotThrow(() -> step.apply(
                request(new MemDirectory(), configuration,
                        SegmentIndexBootstrapMode.CREATE),
                state));

        assertInstanceOf(SegmentIndexMdcLoggingAdapter.class,
                state.getIndexHandle());
        assertNotSame(indexHandle, state.getIndexHandle());
    }

    private SegmentIndexBootstrapState<Integer, String> stateWithIndexHandle(
            final IndexConfiguration<Integer, String> configuration) {
        final SegmentIndexBootstrapState<Integer, String> state =
                new SegmentIndexBootstrapState<>();
        state.setConfiguration(effectiveConfiguration(configuration));
        state.setIndexHandle(indexHandle);
        return state;
    }
}
