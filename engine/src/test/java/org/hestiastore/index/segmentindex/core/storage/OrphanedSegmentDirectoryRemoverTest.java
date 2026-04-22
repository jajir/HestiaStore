package org.hestiastore.index.segmentindex.core.storage;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

class OrphanedSegmentDirectoryRemoverTest {

    @Test
    void remove_retriesUntilSegmentBecomesAvailable() {
        @SuppressWarnings("unchecked")
        final SegmentRegistry<Integer, String> segmentRegistry =
                mock(SegmentRegistry.class);
        final Logger logger = mock(Logger.class);
        final SegmentId segmentId = SegmentId.of(3);
        when(segmentRegistry.deleteSegmentIfAvailable(segmentId))
                .thenReturn(false, true);
        final OrphanedSegmentDirectoryRemover<Integer, String> remover =
                new OrphanedSegmentDirectoryRemover<>(logger, segmentRegistry,
                        new IndexRetryPolicy(2, 1));

        remover.remove(segmentId);

        verify(segmentRegistry).deleteSegmentIfAvailable(segmentId);
        verify(segmentRegistry).deleteSegmentIfAvailable(segmentId);
    }

    @Test
    void remove_swallowsIndexExceptionAndStopsRetrying() {
        @SuppressWarnings("unchecked")
        final SegmentRegistry<Integer, String> segmentRegistry =
                mock(SegmentRegistry.class);
        final Logger logger = mock(Logger.class);
        final SegmentId segmentId = SegmentId.of(3);
        when(segmentRegistry.deleteSegmentIfAvailable(segmentId))
                .thenThrow(new IndexException("boom"));
        final OrphanedSegmentDirectoryRemover<Integer, String> remover =
                new OrphanedSegmentDirectoryRemover<>(logger, segmentRegistry,
                        new IndexRetryPolicy(2, 1));

        remover.remove(segmentId);

        verify(segmentRegistry).deleteSegmentIfAvailable(segmentId);
        verify(logger, never()).info(
                "Deleted orphaned segment directory '{}' during recovery/consistency cleanup.",
                segmentId);
    }
}
