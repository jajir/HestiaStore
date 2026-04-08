package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.function.Supplier;

import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DirectSegmentReadCoordinatorTest {

    @Mock
    private KeyToSegmentMapSynchronizedAdapter<Integer> keyToSegmentMap;

    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;

    @Mock
    private StableSegmentGateway<Integer, String> stableSegmentGateway;

    @Mock
    private BackgroundSplitCoordinator<Integer, String> backgroundSplitCoordinator;

    @Mock
    private IndexRetryPolicy retryPolicy;

    private DirectSegmentReadCoordinator<Integer, String> coordinator;

    @BeforeEach
    void setUp() {
        stubAdmissionRunner();
        coordinator = new DirectSegmentReadCoordinator<>(keyToSegmentMap,
                segmentRegistry, stableSegmentGateway,
                backgroundSplitCoordinator, retryPolicy);
    }

    @Test
    void get_readsValueDirectlyFromStableSegments() {
        when(stableSegmentGateway.get(10)).thenReturn(IndexResult.ok("ten"));

        final IndexResult<String> result = coordinator.get(10);

        assertEquals(IndexResultStatus.OK, result.getStatus());
        assertEquals("ten", result.getValue());
        verify(stableSegmentGateway).get(10);
    }

    @Test
    void get_returnsBusyWhenStableReadCannotProceed() {
        when(stableSegmentGateway.get(10)).thenReturn(IndexResult.busy());

        final IndexResult<String> result = coordinator.get(10);

        assertEquals(IndexResultStatus.BUSY, result.getStatus());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void stubAdmissionRunner() {
        doAnswer(invocation -> ((Supplier) invocation.getArgument(0)).get())
                .when(backgroundSplitCoordinator)
                .runWithSharedSplitAdmission(any());
    }
}
