package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.function.Supplier;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.partition.PartitionRuntime;
import org.hestiastore.index.segmentindex.partition.PartitionRuntimeLimits;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PartitionReadCoordinatorTest {

    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;

    @Mock
    private BackgroundSplitCoordinator<Integer, String> backgroundSplitCoordinator;

    @Mock
    private Segment<Integer, String> segment;

    private TypeDescriptorShortString valueTypeDescriptor;
    private Directory directory;
    private KeyToSegmentMap<Integer> keyToSegmentMap;
    private KeyToSegmentMapSynchronizedAdapter<Integer> synchronizedKeyToSegmentMap;
    private PartitionRuntime<Integer, String> partitionRuntime;
    private StableSegmentGateway<Integer, String> stableSegmentGateway;
    private PartitionReadCoordinator<Integer, String> coordinator;

    @BeforeEach
    void setUp() {
        valueTypeDescriptor = new TypeDescriptorShortString();
        directory = new MemDirectory();
        keyToSegmentMap = new KeyToSegmentMap<>(directory,
                new TypeDescriptorInteger());
        synchronizedKeyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                keyToSegmentMap);
        partitionRuntime = new PartitionRuntime<>(Integer::compareTo);
        stableSegmentGateway = new StableSegmentGateway<>(
                synchronizedKeyToSegmentMap,
                segmentRegistry);
        coordinator = new PartitionReadCoordinator<>(
                synchronizedKeyToSegmentMap, partitionRuntime, segmentRegistry,
                stableSegmentGateway, backgroundSplitCoordinator,
                new TypeDescriptorInteger(), valueTypeDescriptor,
                new IndexRetryPolicy(1, 10));
    }

    @AfterEach
    void tearDown() {
        if (synchronizedKeyToSegmentMap != null
                && !synchronizedKeyToSegmentMap.wasClosed()) {
            synchronizedKeyToSegmentMap.close();
        }
    }

    @Test
    void get_prefersOverlayValueAndHidesTombstone() {
        stubAdmissionRunner();
        final SegmentId segmentId = keyToSegmentMap.insertKeyToSegment(10);
        partitionRuntime.write(segmentId, 10, "overlay",
                new PartitionRuntimeLimits(4, 2, 8, 16));

        final IndexResult<String> visible = coordinator.get(10);

        assertEquals(IndexResultStatus.OK, visible.getStatus());
        assertEquals("overlay", visible.getValue());
        verifyNoInteractions(segmentRegistry);

        partitionRuntime.write(segmentId, 10, valueTypeDescriptor.getTombstone(),
                new PartitionRuntimeLimits(4, 2, 8, 16));
        final IndexResult<String> tombstoned = coordinator.get(10);

        assertEquals(IndexResultStatus.OK, tombstoned.getStatus());
        assertNull(tombstoned.getValue());
    }

    @Test
    void openWindowIterator_mergesStableEntriesWithOverlayAndTombstones() {
        final SegmentId segmentId = keyToSegmentMap.insertKeyToSegment(3);
        when(segmentRegistry.getSegment(segmentId))
                .thenReturn(SegmentRegistryResult.ok(segment));
        when(segment.openIterator(SegmentIteratorIsolation.FAIL_FAST))
                .thenReturn(SegmentResult.ok(EntryIterator.make(List.of(
                        Entry.of(1, "stable-1"), Entry.of(2, "stable-2"),
                        Entry.of(3, "stable-3")).iterator())));
        partitionRuntime.write(segmentId, 2, "overlay-2",
                new PartitionRuntimeLimits(4, 2, 8, 16));
        partitionRuntime.write(segmentId, 3, valueTypeDescriptor.getTombstone(),
                new PartitionRuntimeLimits(4, 2, 8, 16));

        final List<Entry<Integer, String>> entries;
        try (EntryIterator<Integer, String> iterator = coordinator
                .openWindowIterator(SegmentWindow.unbounded(),
                        SegmentIteratorIsolation.FAIL_FAST)) {
            entries = toList(iterator);
        }

        assertEquals(List.of(Entry.of(1, "stable-1"),
                Entry.of(2, "overlay-2")), entries);
    }

    @Test
    void get_returnsBusyWhenTopologyChangesDuringStableRead() {
        stubAdmissionRunner();
        final SegmentId segmentId = keyToSegmentMap.insertKeyToSegment(10);
        when(segmentRegistry.getSegment(segmentId))
                .thenReturn(SegmentRegistryResult.ok(segment));
        when(segment.get(10)).thenAnswer(invocation -> {
            synchronizedKeyToSegmentMap.updateSegmentMaxKey(segmentId, 11);
            return SegmentResult.ok("stable-10");
        });

        final IndexResult<String> result = coordinator.get(10);

        assertEquals(IndexResultStatus.BUSY, result.getStatus());
        assertNull(result.getValue());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void stubAdmissionRunner() {
        doAnswer(invocation -> ((Supplier) invocation.getArgument(0)).get())
                .when(backgroundSplitCoordinator)
                .runWithStableWriteAdmission(any());
    }

    private static <K, V> List<Entry<K, V>> toList(
            final EntryIterator<K, V> iterator) {
        final var entries = new java.util.ArrayList<Entry<K, V>>();
        while (iterator.hasNext()) {
            entries.add(iterator.next());
        }
        return entries;
    }
}
