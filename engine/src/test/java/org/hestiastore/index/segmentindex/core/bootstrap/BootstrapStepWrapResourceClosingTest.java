package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.request;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.mockito.Mockito.verify;

import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.session.IndexInternal;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexResourceClosingAdapter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BootstrapStepWrapResourceClosingTest {

    @Mock
    private IndexInternal<Integer, String> managedIndex;

    @Mock
    private ExecutorRegistry executorRegistry;

    private BootstrapStepWrapResourceClosing<Integer, String> step;

    @BeforeEach
    void setUp() {
        step = new BootstrapStepWrapResourceClosing<>();
    }

    @Test
    void apply_wrapsManagedIndexWithResourceClosingAdapter() {
        final SegmentIndexBootstrapState<Integer, String> state =
                new SegmentIndexBootstrapState<>();
        state.setManagedIndex(managedIndex);
        state.setExecutorRegistry(executorRegistry);

        assertDoesNotThrow(() -> step.apply(
                request(new MemDirectory(), SegmentIndexBootstrapMode.CREATE),
                state));

        assertInstanceOf(SegmentIndexResourceClosingAdapter.class,
                state.getIndex());
        assertNotSame(managedIndex, state.getIndex());
    }

    @Test
    void returnedIndexCloseClosesManagedIndexAndExecutorRegistry() {
        final SegmentIndexBootstrapState<Integer, String> state =
                new SegmentIndexBootstrapState<>();
        state.setManagedIndex(managedIndex);
        state.setExecutorRegistry(executorRegistry);
        step.apply(request(new MemDirectory(), SegmentIndexBootstrapMode.OPEN),
                state);

        assertDoesNotThrow(() -> state.getIndex().close());

        verify(managedIndex).close();
        verify(executorRegistry).close();
    }
}
