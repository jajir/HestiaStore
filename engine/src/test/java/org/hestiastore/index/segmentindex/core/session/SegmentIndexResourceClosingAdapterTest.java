package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.maintenance.SegmentIndexMaintenance;
import org.hestiastore.index.segmentindex.tuning.RuntimeConfiguration;
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

    private static final class NoopSegmentIndex extends AbstractCloseableResource
            implements SegmentIndex<String, String> {

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
        protected void doClose() {
            // no-op
        }

        @Override
        public IndexRuntimeMonitoring runtimeMonitoring() {
            return mock(IndexRuntimeMonitoring.class);
        }

        @Override
        public RuntimeConfiguration runtimeTuning() {
            return mock(RuntimeConfiguration.class);
        }

        @Override
        public SegmentIndexMaintenance maintenance() {
            return mock(SegmentIndexMaintenance.class);
        }
    }
}
