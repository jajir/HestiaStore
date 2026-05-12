package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.effectiveConfiguration;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.request;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.stateWithRuntimeInputs;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;

import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionResources;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BootstrapStepCreateRuntimeTest {

    @Mock
    private SegmentIndexSessionResources<Integer, String> sessionResources;

    @Mock
    private ExecutorRegistry executorRegistry;

    private BootstrapStepCreateRuntime<Integer, String> step;

    @BeforeEach
    void setUp() {
        step = new BootstrapStepCreateRuntime<>(sessionResources);
    }

    @Test
    void constructor_rejectsNullSessionResources() {
        assertThrows(IllegalArgumentException.class,
                () -> new BootstrapStepCreateRuntime<Integer, String>(null));
    }

    @Test
    void apply_createsRuntimeThroughSessionResources() {
        final MemDirectory directory = new MemDirectory();
        final EffectiveIndexConfiguration<Integer, String> configuration =
                effectiveConfiguration("bootstrap-step-runtime");
        final SegmentIndexBootstrapState<Integer, String> state =
                stateWithRuntimeInputs(configuration, executorRegistry);

        assertDoesNotThrow(() -> step.apply(
                request(directory, SegmentIndexBootstrapMode.CREATE), state));

        verify(sessionResources).createRuntime(directory,
                state.getKeyTypeDescriptor(), state.getValueTypeDescriptor(),
                configuration, executorRegistry);
    }

    @Test
    void closeResource_closesRuntimeAfterFailedInitialization() {
        assertDoesNotThrow(step::closeResource);

        verify(sessionResources).closeRuntimeAfterFailedInitialization();
    }
}
