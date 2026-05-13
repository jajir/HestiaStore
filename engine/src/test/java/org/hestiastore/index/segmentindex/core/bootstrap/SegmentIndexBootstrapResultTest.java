package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.hestiastore.index.segmentindex.core.session.IndexInternal;
import org.junit.jupiter.api.Test;

class SegmentIndexBootstrapResultTest {

    @Test
    void createdRequiresIndex() {
        final IndexInternal<Integer, String> index = mockIndex();

        final SegmentIndexBootstrapResult<Integer, String> result =
                SegmentIndexBootstrapResult.created(index);

        assertEquals(SegmentIndexBootstrapStatus.CREATED, result.status());
        assertSame(index, result.requireIndex());
        assertSame(index, result.index().orElseThrow());
        assertThrows(IllegalArgumentException.class,
                () -> SegmentIndexBootstrapResult.created(null));
    }

    @Test
    void openedRequiresIndex() {
        final IndexInternal<Integer, String> index = mockIndex();

        final SegmentIndexBootstrapResult<Integer, String> result =
                SegmentIndexBootstrapResult.opened(index);

        assertEquals(SegmentIndexBootstrapStatus.OPENED, result.status());
        assertSame(index, result.requireIndex());
        assertSame(index, result.index().orElseThrow());
        assertThrows(IllegalArgumentException.class,
                () -> SegmentIndexBootstrapResult.opened(null));
    }

    @Test
    void notFoundDoesNotContainIndex() {
        final SegmentIndexBootstrapResult<Integer, String> result =
                SegmentIndexBootstrapResult.notFound();

        assertEquals(SegmentIndexBootstrapStatus.NOT_FOUND, result.status());
        assertTrue(result.index().isEmpty());
        assertThrows(IllegalStateException.class, result::requireIndex);
    }

    @SuppressWarnings("unchecked")
    private static IndexInternal<Integer, String> mockIndex() {
        return mock(IndexInternal.class);
    }
}
