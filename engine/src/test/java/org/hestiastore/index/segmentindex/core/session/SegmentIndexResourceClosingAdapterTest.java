package org.hestiastore.index.segmentindex.core.session;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
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
        public void compact() {
            // no-op
        }

        @Override
        public void flush() {
            // no-op
        }

        @Override
        public void compactAndWait() {
            // no-op
        }

        @Override
        public void flushAndWait() {
            // no-op
        }

        @Override
        public Stream<Entry<String, String>> getStream(
                final SegmentWindow segmentWindows) {
            return Stream.empty();
        }

        @Override
        public void checkAndRepairConsistency() {
            // no-op
        }

        @Override
        public IndexConfiguration<String, String> getConfiguration() {
            return IndexConfiguration.<String, String>builder()//
                    .identity(identity -> identity.keyClass(String.class))//
                    .identity(identity -> identity.valueClass(String.class))//
                    .identity(identity -> identity.name("noop-index"))//
                    .build();
        }

        @Override
        public SegmentIndexState getState() {
            return wasClosed() ? SegmentIndexState.CLOSED
                    : SegmentIndexState.READY;
        }

        @Override
        protected void doClose() {
            // no-op
        }
    }
}
