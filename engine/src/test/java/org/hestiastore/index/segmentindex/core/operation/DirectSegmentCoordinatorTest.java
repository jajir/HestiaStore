package org.hestiastore.index.segmentindex.core.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.function.Supplier;

import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapImpl;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.core.split.BackgroundSplitCoordinator;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DirectSegmentCoordinatorTest {

    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;

    @Mock
    private StableSegmentGateway<Integer, String> stableSegmentGateway;

    @Mock
    private BackgroundSplitCoordinator<Integer, String> backgroundSplitCoordinator;

    @Mock
    private IndexRetryPolicy retryPolicy;

    private Directory directory;
    private KeyToSegmentMap<Integer> synchronizedKeyToSegmentMap;
    private DirectSegmentCoordinator<Integer, String> coordinator;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        synchronizedKeyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                new KeyToSegmentMapImpl<>(directory,
                        new TypeDescriptorInteger()));
        stubAdmissionRunner();
        coordinator = new DirectSegmentCoordinator<>(synchronizedKeyToSegmentMap,
                segmentRegistry, stableSegmentGateway,
                backgroundSplitCoordinator, retryPolicy);
    }

    @AfterEach
    void tearDown() {
        if (synchronizedKeyToSegmentMap != null
                && !synchronizedKeyToSegmentMap.wasClosed()) {
            synchronizedKeyToSegmentMap.close();
        }
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

    @Test
    void put_createsBootstrapRouteAndWritesDirectlyToStableSegment() {
        when(stableSegmentGateway.put(SegmentId.of(0), 11, "v11"))
                .thenReturn(IndexResult.ok());

        final IndexResult<Void> result = coordinator.put(11, "v11");

        assertEquals(IndexResultStatus.OK, result.getStatus());
        assertEquals(SegmentId.of(0),
                synchronizedKeyToSegmentMap.findSegmentIdForKey(11));
        verify(stableSegmentGateway).put(SegmentId.of(0), 11, "v11");
    }

    @Test
    void put_returnsBusyWhenStableSegmentRejectsWrite() {
        synchronizedKeyToSegmentMap.extendMaxKeyIfNeeded(10);
        when(stableSegmentGateway.put(SegmentId.of(0), 10, "v10"))
                .thenReturn(IndexResult.busy());

        final IndexResult<Void> result = coordinator.put(10, "v10");

        assertEquals(IndexResultStatus.BUSY, result.getStatus());
    }

    @Test
    void put_returnsBusyWhenRouteIsSplitBlocked() {
        synchronizedKeyToSegmentMap.extendMaxKeyIfNeeded(10);
        when(backgroundSplitCoordinator.isSplitBlocked(SegmentId.of(0)))
                .thenReturn(true);

        final IndexResult<Void> result = coordinator.put(10, "v10");

        assertEquals(IndexResultStatus.BUSY, result.getStatus());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void stubAdmissionRunner() {
        doAnswer(invocation -> ((Supplier) invocation.getArgument(0)).get())
                .when(backgroundSplitCoordinator)
                .runWithSharedSplitAdmission(any());
    }
}
