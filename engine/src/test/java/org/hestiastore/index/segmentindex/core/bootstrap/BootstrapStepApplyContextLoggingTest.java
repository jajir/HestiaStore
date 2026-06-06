package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.request;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.when;

import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.core.session.IndexContextLoggingAdapter;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionHandle;
import org.hestiastore.index.segmentindex.logging.IndexMdcCallWrapper;
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
    private SegmentIndexSessionHandle<Integer, String> internalIndex;

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
    void apply_usesInternalIndexDirectlyWhenMdcCallWrapperIsMissing() {
        final SegmentIndexBootstrapState<Integer, String> state =
                stateWithInternalIndex();

        assertDoesNotThrow(() -> step.apply(
                request(new MemDirectory(), SegmentIndexBootstrapMode.CREATE),
                state));

        assertSame(internalIndex, state.getManagedIndex());
    }

    @Test
    void apply_wrapsInternalIndexWhenMdcCallWrapperExists() {
        when(internalIndex.runtimeTuning()).thenReturn(runtimeTuning);
        when(internalIndex.runtimeMonitoring()).thenReturn(runtimeMonitoring);
        when(internalIndex.maintenance()).thenReturn(maintenance);
        final SegmentIndexBootstrapState<Integer, String> state =
                stateWithInternalIndex();
        state.setIndexMdcCallWrapper(new IndexMdcCallWrapper(
                "bootstrap-step-context-logging"));

        assertDoesNotThrow(() -> step.apply(
                request(new MemDirectory(), SegmentIndexBootstrapMode.CREATE),
                state));

        assertInstanceOf(IndexContextLoggingAdapter.class,
                state.getManagedIndex());
        assertNotSame(internalIndex, state.getManagedIndex());
    }

    private SegmentIndexBootstrapState<Integer, String> stateWithInternalIndex() {
        final SegmentIndexBootstrapState<Integer, String> state =
                new SegmentIndexBootstrapState<>();
        state.setInternalIndex(internalIndex);
        return state;
    }
}
