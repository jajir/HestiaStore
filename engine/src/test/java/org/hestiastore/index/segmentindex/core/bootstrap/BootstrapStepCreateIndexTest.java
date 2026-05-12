package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.effectiveConfiguration;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.request;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.stateWithConfiguration;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.core.session.IndexInternal;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionResources;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BootstrapStepCreateIndexTest {

    @Mock
    private SegmentIndexSessionResources<Integer, String> sessionResources;

    @Mock
    private IndexInternal<Integer, String> internalIndex;

    private BootstrapStepCreateIndex<Integer, String> step;

    @BeforeEach
    void setUp() {
        step = new BootstrapStepCreateIndex<>(sessionResources);
    }

    @Test
    void constructor_rejectsNullSessionResources() {
        assertThrows(IllegalArgumentException.class,
                () -> new BootstrapStepCreateIndex<Integer, String>(null));
    }

    @Test
    void apply_createsInternalIndexThroughSessionResources() {
        final EffectiveIndexConfiguration<Integer, String> configuration =
                effectiveConfiguration("bootstrap-step-create-index");
        final SegmentIndexBootstrapState<Integer, String> state =
                stateWithConfiguration(configuration);
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        state.setKeyTypeDescriptor(keyDescriptor);
        when(sessionResources.createIndex(configuration, keyDescriptor))
                .thenReturn(internalIndex);

        assertDoesNotThrow(() -> step.apply(
                request(new MemDirectory(), SegmentIndexBootstrapMode.CREATE),
                state));

        assertSame(internalIndex, state.getInternalIndex());
        verify(sessionResources).createIndex(configuration, keyDescriptor);
    }
}
