package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.request;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.verify;

import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.core.session.IndexInternal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BootstrapStepCompleteStartupTest {

    @Mock
    private IndexInternal<Integer, String> internalIndex;

    private BootstrapStepCompleteStartup<Integer, String> step;

    @BeforeEach
    void setUp() {
        step = new BootstrapStepCompleteStartup<>();
    }

    @Test
    void apply_completesStartupOnInternalIndex() {
        final SegmentIndexBootstrapState<Integer, String> state =
                new SegmentIndexBootstrapState<>();
        state.setInternalIndex(internalIndex);

        assertDoesNotThrow(() -> step.apply(
                request(new MemDirectory(), SegmentIndexBootstrapMode.OPEN),
                state));

        verify(internalIndex).completeStartup();
    }
}
