package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.maintenance.SegmentIndexMaintenance;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.junit.jupiter.api.Test;

class SegmentIndexResourceClosingAdapterTest {

    @Test
    void closeClosesDelegateAndExecutorRegistry() {
        final NoopSegmentIndex delegate = new NoopSegmentIndex();
        final ExecutorRegistry executorRegistry = mock(ExecutorRegistry.class);

        try (SegmentIndexResourceClosingAdapter<String, String> adapter = new SegmentIndexResourceClosingAdapter<>(
                delegate, executorRegistry)) {
            // close triggered by try-with-resources
        }

        assertTrue(delegate.wasClosed(), "Delegate index should be closed");
        verify(executorRegistry).close();
    }

    @Test
    void delegatesInternalOperations() {
        final IndexInternal<String, String> delegate = mockIndex();
        final ExecutorRegistry executorRegistry = mock(ExecutorRegistry.class);
        final SegmentWindow window = SegmentWindow.unbounded();
        final EntryIterator<String, String> iterator = mockIterator();
        when(delegate.openSegmentIterator(window)).thenReturn(iterator);

        try (SegmentIndexResourceClosingAdapter<String, String> adapter = new SegmentIndexResourceClosingAdapter<>(
                delegate,
                executorRegistry)) {
            assertSame(iterator, adapter.openSegmentIterator(window));
            adapter.completeStartup();
        }

        verify(delegate).openSegmentIterator(window);
        verify(delegate).completeStartup();
    }

    private static final class NoopSegmentIndex extends AbstractCloseableResource
            implements IndexInternal<String, String> {

        @Override
        public void put(final String key, final String value) {
            // no-op
        }

        @Override
        public String get(final String key) {
            return null;
        }

        @Override
        public void delete(final String key) {
            // no-op
        }

        @Override
        public Stream<Entry<String, String>> getStream(
                final SegmentWindow segmentWindows) {
            return Stream.empty();
        }

        @Override
        public EntryIterator<String, String> openSegmentIterator(
                final SegmentWindow segmentWindows) {
            return mockIterator();
        }

        @Override
        public void completeStartup() {
            // no-op
        }

        @Override
        protected void doClose() {
            // no-op
        }

        @Override
        public IndexRuntimeMonitoring runtimeMonitoring() {
            return mock(IndexRuntimeMonitoring.class);
        }

        @Override
        public RuntimeTuning runtimeTuning() {
            return mock(RuntimeTuning.class);
        }

        @Override
        public SegmentIndexMaintenance maintenance() {
            return mock(SegmentIndexMaintenance.class);
        }
    }

    @SuppressWarnings("unchecked")
    private static IndexInternal<String, String> mockIndex() {
        return mock(IndexInternal.class);
    }

    @SuppressWarnings("unchecked")
    private static EntryIterator<String, String> mockIterator() {
        return mock(EntryIterator.class);
    }
}
