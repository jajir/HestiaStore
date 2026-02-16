package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.directory.Directory;
import org.junit.jupiter.api.Test;

class IndexDirectoryClosingAdapterTest {

    @Test
    void close_closes_delegate_and_async_directory() {
        final Directory asyncDirectory = (Directory) mock(Directory.class,
                withSettings().extraInterfaces(CloseableResource.class));
        final NoopSegmentIndex delegate = new NoopSegmentIndex();

        try (IndexDirectoryClosingAdapter<String, String> adapter = new IndexDirectoryClosingAdapter<>(
                delegate, asyncDirectory)) {
            // close triggered by try-with-resources
        }

        assertTrue(delegate.wasClosed(), "Delegate index should be closed");
        verify((CloseableResource) asyncDirectory).close();
    }

    private static final class NoopSegmentIndex extends AbstractCloseableResource
            implements SegmentIndex<String, String> {

        @Override
        public void put(final String key, final String value) {
            // no-op
        }

        @Override
        public CompletionStage<Void> putAsync(final String key,
                final String value) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public String get(final String key) {
            return null;
        }

        @Override
        public CompletionStage<String> getAsync(final String key) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void delete(final String key) {
            // no-op
        }

        @Override
        public CompletionStage<Void> deleteAsync(final String key) {
            return CompletableFuture.completedFuture(null);
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
                    .withKeyClass(String.class)//
                    .withValueClass(String.class)//
                    .withName("noop-index")//
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
