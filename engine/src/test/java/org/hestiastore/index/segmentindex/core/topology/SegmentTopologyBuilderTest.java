package org.hestiastore.index.segmentindex.core.topology;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapImpl;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.mapping.Snapshot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SegmentTopologyBuilderTest {

    private KeyToSegmentMap<Integer> keyToSegmentMap;

    @BeforeEach
    void setUp() {
        keyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                new KeyToSegmentMapImpl<>(new MemDirectory(),
                        new TypeDescriptorInteger()));
    }

    @AfterEach
    void tearDown() {
        if (keyToSegmentMap != null && !keyToSegmentMap.wasClosed()) {
            keyToSegmentMap.close();
        }
    }

    @Test
    void buildCreatesTopologyFromSnapshot() {
        keyToSegmentMap.extendMaxKeyIfNeeded(100);
        final Snapshot<Integer> snapshot = keyToSegmentMap.snapshot();

        final SegmentTopology<Integer> topology = SegmentTopology
                .<Integer>builder().snapshot(snapshot).build();

        final SegmentTopology.RouteLeaseResult leaseResult = topology
                .tryAcquire(SegmentId.of(0), snapshot.version());

        assertEquals(snapshot.version(), topology.version());
        assertTrue(leaseResult.isAcquired());
        try (SegmentTopology.RouteLease lease = leaseResult.lease()) {
            assertEquals(SegmentId.of(0), lease.segmentId());
        }
    }

    @Test
    void buildRejectsMissingSnapshot() {
        final SegmentTopologyBuilder<Integer> builder = SegmentTopology
                .builder();

        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    void snapshotRejectsNullSnapshot() {
        final SegmentTopologyBuilder<Integer> builder = SegmentTopology
                .builder();

        assertThrows(IllegalArgumentException.class,
                () -> builder.snapshot(null));
    }
}
