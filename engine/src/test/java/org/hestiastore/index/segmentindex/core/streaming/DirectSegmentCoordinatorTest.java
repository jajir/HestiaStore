package org.hestiastore.index.segmentindex.core.streaming;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DirectSegmentCoordinatorTest {

    @Mock
    private KeyToSegmentMap<Integer> keyToSegmentMap;

    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;

    @Mock
    private IndexRetryPolicy retryPolicy;

    private DirectSegmentCoordinator<Integer, String> coordinator;

    @BeforeEach
    void setUp() {
        coordinator = new DirectSegmentCoordinator<>(keyToSegmentMap,
                segmentRegistry, retryPolicy);
    }

    @Test
    void openWindowIteratorReturnsIteratorForMappedSegmentIds() {
        final SegmentWindow window = SegmentWindow.unbounded();
        when(keyToSegmentMap.getSegmentIds(window)).thenReturn(List.of());

        final EntryIterator<Integer, String> iterator = coordinator
                .openWindowIterator(window, SegmentIteratorIsolation.FAIL_FAST);

        assertNotNull(iterator);
        assertFalse(iterator.hasNext());
        iterator.close();
    }
}
