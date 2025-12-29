package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.junit.jupiter.api.Test;

class IndexDirectoryClosingAdapterTest {

    @Test
    void close_closes_delegate_and_async_directory() {
        final AsyncDirectory asyncDirectory = AsyncDirectoryAdapter
                .wrap(new MemDirectory());
        final NoopSegmentIndex delegate = new NoopSegmentIndex();

        try (IndexDirectoryClosingAdapter<String, String> adapter = new IndexDirectoryClosingAdapter<>(
                delegate, asyncDirectory)) {
            // close triggered by try-with-resources
        }

        assertTrue(delegate.wasClosed(), "Delegate index should be closed");
        assertTrue(asyncDirectory.wasClosed(),
                "AsyncDirectory should be closed");
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
        protected void doClose() {
            // no-op
        }
    }
}
