package org.hestiastore.index.segmentindex.core.routing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapImpl;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopology;
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
    private IndexRetryPolicy retryPolicy;

    private Directory directory;
    private KeyToSegmentMap<Integer> synchronizedKeyToSegmentMap;
    private SegmentTopology<Integer> segmentTopology;
    private DirectSegmentCoordinator<Integer, String> coordinator;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        synchronizedKeyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                new KeyToSegmentMapImpl<>(directory,
                        new TypeDescriptorInteger()));
        segmentTopology = SegmentTopology.<Integer>builder()
                .snapshot(synchronizedKeyToSegmentMap.snapshot()).build();
        coordinator = new DirectSegmentCoordinator<>(synchronizedKeyToSegmentMap,
                segmentRegistry, stableSegmentGateway,
                segmentTopology, retryPolicy);
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
        synchronizedKeyToSegmentMap.extendMaxKeyIfNeeded(10);
        segmentTopology.reconcile(synchronizedKeyToSegmentMap.snapshot());
        when(stableSegmentGateway.get(SegmentId.of(0), 10))
                .thenReturn(IndexResult.ok("ten"));

        final IndexResult<String> result = coordinator.get(10);

        assertEquals(IndexResultStatus.OK, result.getStatus());
        assertEquals("ten", result.getValue());
        verify(stableSegmentGateway).get(SegmentId.of(0), 10);
    }

    @Test
    void get_retriesBusyStableReadUntilRetryPolicyFails() {
        synchronizedKeyToSegmentMap.extendMaxKeyIfNeeded(10);
        segmentTopology.reconcile(synchronizedKeyToSegmentMap.snapshot());
        when(stableSegmentGateway.get(SegmentId.of(0), 10))
                .thenReturn(IndexResult.busy());
        doThrow(new IndexException("timeout")).when(retryPolicy)
                        .backoffOrThrow(0L, "get", SegmentId.of(0));

        assertThrows(IndexException.class, () -> coordinator.get(10));
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
    void put_retriesBusyStableSegmentUnderRouteLease() {
        synchronizedKeyToSegmentMap.extendMaxKeyIfNeeded(10);
        segmentTopology.reconcile(synchronizedKeyToSegmentMap.snapshot());
        when(stableSegmentGateway.put(SegmentId.of(0), 10, "v10"))
                .thenReturn(IndexResult.busy(), IndexResult.ok());

        final IndexResult<Void> result = coordinator.put(10, "v10");

        assertEquals(IndexResultStatus.OK, result.getStatus());
        verify(retryPolicy).backoffOrThrow(0L, "put", SegmentId.of(0));
    }

    @Test
    void put_returnsBusyWhenRouteIsSplitBlocked() {
        synchronizedKeyToSegmentMap.extendMaxKeyIfNeeded(10);
        segmentTopology.reconcile(synchronizedKeyToSegmentMap.snapshot());
        segmentTopology.tryBeginDrain(SegmentId.of(0));

        final IndexResult<Void> result = coordinator.put(10, "v10");

        assertEquals(IndexResultStatus.BUSY, result.getStatus());
    }
}
