package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.hestiastore.index.segmentindex.core.bootstrap.BootstrapStepTestSupport.request;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexResourceClosingAdapter;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionResource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BootstrapStepWrapResourceClosingTest {

    @Mock
    private SegmentIndexSessionResource<Integer, String> indexHandle;

    @Mock
    private ExecutorRegistry executorRegistry;

    private BootstrapStepWrapResourceClosing<Integer, String> step;

    @BeforeEach
    void setUp() {
        step = new BootstrapStepWrapResourceClosing<>();
    }

    @Test
    void apply_wrapsIndexHandleWithResourceClosingAdapter() {
        final SegmentIndexBootstrapState<Integer, String> state =
                new SegmentIndexBootstrapState<>();
        state.setIndexHandle(indexHandle);
        state.setExecutorRegistry(executorRegistry);

        assertDoesNotThrow(() -> step.apply(
                request(new MemDirectory(), SegmentIndexBootstrapMode.CREATE),
                state));

        assertInstanceOf(SegmentIndexResourceClosingAdapter.class,
                state.getIndex());
        assertNotSame(indexHandle, state.getIndex());
    }

    @Test
    void returnedIndexCloseClosesIndexHandleOnly() {
        final SegmentIndexBootstrapState<Integer, String> state =
                new SegmentIndexBootstrapState<>();
        state.setIndexHandle(indexHandle);
        state.setExecutorRegistry(executorRegistry);
        step.apply(request(new MemDirectory(), SegmentIndexBootstrapMode.OPEN),
                state);

        assertDoesNotThrow(() -> state.getIndex().close());

        verify(indexHandle).close();
        verify(executorRegistry, never()).close();
    }
}
