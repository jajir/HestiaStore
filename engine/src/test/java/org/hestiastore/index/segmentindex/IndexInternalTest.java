package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.junit.jupiter.api.Test;

class IndexInternalTest {

    @Test
    void defaultGetStreamThrows() {
        try (IndexInternal<String, String> index = new StubIndexInternal()) {
            final SegmentWindow window = SegmentWindow.unbounded();

            final UnsupportedOperationException ex = assertThrows(
                    UnsupportedOperationException.class,
                    () -> index.getStream(window));
            assertEquals("should be definec in the concrete class",
                    ex.getMessage());
        }
    }

    private static final class StubIndexInternal
            implements IndexInternal<String, String> {

        @Override
        public EntryIterator<String, String> openSegmentIterator(
                SegmentWindow segmentWindows) {
            return EntryIterator
                    .make(List.<Entry<String, String>>of().iterator());
        }

        @Override
        public void put(final String key, final String value) {
            // no-op test stub
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
            // no-op test stub
        }

        @Override
        public CompletionStage<Void> deleteAsync(final String key) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void compact() {
            // no-op test stub
        }

        @Override
        public void compactAndWait() {
            // no-op test stub
        }

        @Override
        public void flush() {
            // no-op test stub
        }

        @Override
        public void flushAndWait() {
            // no-op test stub
        }

        @Override
        public void checkAndRepairConsistency() {
            // no-op test stub
        }

        @Override
        public IndexConfiguration<String, String> getConfiguration() {
            return null;
        }

        @Override
        public SegmentIndexState getState() {
            return SegmentIndexState.READY;
        }

        @Override
        public boolean wasClosed() {
            return false;
        }

        @Override
        public void close() {
            // no-op test stub
        }
    }
}
