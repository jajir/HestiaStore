package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.effectiveConfiguration;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.request;
import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.stateWithConfiguration;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class BootstrapStepResolveTypeDescriptorsTest {

    private final BootstrapStepResolveTypeDescriptors<Integer, String> step =
            new BootstrapStepResolveTypeDescriptors<>();

    @Test
    void apply_resolvesConfiguredKeyAndValueDescriptors() {
        final SegmentIndexBootstrapState<Integer, String> state =
                stateWithConfiguration(effectiveConfiguration(
                        "bootstrap-step-descriptor"));

        assertDoesNotThrow(() -> step.apply(
                request(new MemDirectory(), SegmentIndexBootstrapMode.CREATE),
                state));

        assertInstanceOf(TypeDescriptorInteger.class,
                state.getKeyTypeDescriptor());
        assertInstanceOf(TypeDescriptorShortString.class,
                state.getValueTypeDescriptor());
    }
}
