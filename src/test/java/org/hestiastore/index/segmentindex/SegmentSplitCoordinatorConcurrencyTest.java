package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.sorteddatafile.SortedDataFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentSplitCoordinatorConcurrencyTest {

    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;

    @Mock
    private Segment<Integer, String> segment;

    private SegmentWriterTxFactory<Integer, String> writerTxFactory;
    private KeyToSegmentMapSynchronizedAdapter<Integer> keyToSegmentMap;
    private SegmentSplitCoordinator<Integer, String> coordinator;

    @BeforeEach
    void setUp() {
        final KeyToSegmentMap<Integer> rawKeyMap = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(1)), Entry.of(30, SegmentId.of(4))));
        keyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(rawKeyMap);
        final IndexConfiguration<Integer, String> coordinatorConf = IndexConfiguration
                .<Integer, String>builder().withIndexBusyBackoffMillis(1)
                .withIndexBusyTimeoutMillis(50).build();
        writerTxFactory = id -> {
            throw new IllegalStateException("writerTxFactory not configured");
        };
        coordinator = new SegmentSplitCoordinator<>(coordinatorConf,
                keyToSegmentMap, segmentRegistry, writerTxFactory);
    }

    @AfterEach
    void tearDown() {
        if (keyToSegmentMap != null && !keyToSegmentMap.wasClosed()) {
            keyToSegmentMap.close();
        }
        coordinator = null;
        keyToSegmentMap = null;
        writerTxFactory = null;
    }

    @Test
    void applySplitPlan_returns_busy_when_old_segment_missing() {
        final SegmentSplitApplyPlan<Integer, String> plan = new SegmentSplitApplyPlan<>(
                SegmentId.of(9), SegmentId.of(2), SegmentId.of(3), 1, 5,
                SegmentSplitterResult.SegmentSplittingStatus.SPLIT);
        assertFalse(coordinator.applySplitPlan(plan, segment));
    }

    @Test
    void applySplitPlan_updates_map_when_valid() {
        final SegmentSplitApplyPlan<Integer, String> plan = new SegmentSplitApplyPlan<>(
                SegmentId.of(1), SegmentId.of(2), SegmentId.of(3), 1, 5,
                SegmentSplitterResult.SegmentSplittingStatus.SPLIT);
        assertTrue(coordinator.applySplitPlan(plan, segment));
        assertEquals(List.of(SegmentId.of(2), SegmentId.of(3), SegmentId.of(4)),
                keyToSegmentMap.getSegmentIds());
    }

    private KeyToSegmentMap<Integer> newCacheWithEntries(
            final List<Entry<Integer, SegmentId>> entries) {
        final MemDirectory dir = new MemDirectory();
        final SortedDataFile<Integer, SegmentId> sdf = SortedDataFile
                .<Integer, SegmentId>builder()
                .withAsyncDirectory(AsyncDirectoryAdapter.wrap(dir))
                .withFileName("index.map")
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorSegmentId()).build();
        sdf.openWriterTx()
                .execute(writer -> entries.stream().sorted(
                        (e1, e2) -> Integer.compare(e1.getKey(), e2.getKey()))
                        .forEach(writer::write));
        return new KeyToSegmentMap<>(AsyncDirectoryAdapter.wrap(dir),
                new TypeDescriptorInteger());
    }

}
