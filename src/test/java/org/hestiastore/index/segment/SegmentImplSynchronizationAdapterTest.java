package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.hestiastore.index.EntryIterator;
import org.junit.jupiter.api.Test;

class SegmentImplSynchronizationAdapterTest {

    @Test
    void fullIsolationIterator_blocksWritesUntilClosed() throws Exception {
        @SuppressWarnings("unchecked")
        final Segment<Integer, String> delegate = (Segment<Integer, String>) mock(
                Segment.class);
        final EntryIterator<Integer, String> innerIterator = mock(
                EntryIterator.class);
        when(delegate.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(SegmentResult.ok(innerIterator));
        when(delegate.put(1, "one")).thenReturn(SegmentResult.ok());

        final SegmentImplSynchronizationAdapter<Integer, String> adapter = new SegmentImplSynchronizationAdapter<>(
                delegate);
        final SegmentResult<EntryIterator<Integer, String>> result = adapter
                .openIterator(SegmentIteratorIsolation.FULL_ISOLATION);
        final EntryIterator<Integer, String> iterator = result.getValue();

        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            final Future<SegmentResult<Void>> future = executor
                    .submit(() -> adapter.put(1, "one"));

            assertThrows(TimeoutException.class,
                    () -> future.get(100, TimeUnit.MILLISECONDS));

            iterator.close();

            final SegmentResult<Void> putResult = future.get(1,
                    TimeUnit.SECONDS);
            assertEquals(SegmentResultStatus.OK, putResult.getStatus());
            verify(delegate).put(1, "one");
        } finally {
            executor.shutdownNow();
        }
    }
}
