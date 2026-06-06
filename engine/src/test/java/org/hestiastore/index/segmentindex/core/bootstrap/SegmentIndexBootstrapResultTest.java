package org.hestiastore.index.segmentindex.core.bootstrap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.hestiastore.index.segmentindex.core.session.SegmentIndexResourceClosingAdapter;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionHandle;
import org.junit.jupiter.api.Test;

class SegmentIndexBootstrapResultTest {

    @Test
    void createdRequiresIndex() {
        final SegmentIndexResourceClosingAdapter<Integer, String> index =
                mockIndex();

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
        final SegmentIndexResourceClosingAdapter<Integer, String> index =
                mockIndex();

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
    private static SegmentIndexResourceClosingAdapter<Integer, String> mockIndex() {
        return new SegmentIndexResourceClosingAdapter<>(
                mock(SegmentIndexSessionHandle.class));
    }
}
