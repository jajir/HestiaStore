package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.request;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionResources;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BootstrapStepCreateSessionInfrastructureTest {

    @Mock
    private SegmentIndexSessionResources<Integer, String> sessionResources;

    private BootstrapStepCreateSessionInfrastructure<Integer, String> step;

    @BeforeEach
    void setUp() {
        step = new BootstrapStepCreateSessionInfrastructure<>(
                sessionResources);
    }

    @Test
    void constructor_rejectsNullSessionResources() {
        assertThrows(IllegalArgumentException.class,
                () -> new BootstrapStepCreateSessionInfrastructure<Integer, String>(
                        null));
    }

    @Test
    void apply_createsSessionInfrastructureThroughSessionResources() {
        assertDoesNotThrow(() -> step.apply(
                request(new MemDirectory(), SegmentIndexBootstrapMode.CREATE),
                new SegmentIndexBootstrapState<>()));

        verify(sessionResources).setSessionInfrastructure(any());
    }
}
