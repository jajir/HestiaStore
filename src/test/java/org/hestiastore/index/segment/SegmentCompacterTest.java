package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.Entry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentCompacterTest {

    @Mock
    private SegmentCore<Integer, String> segment;

    @Mock
    private VersionController versionController;

    @Mock
    private SegmentFiles<Integer, String> files;

    private SegmentCompacter<Integer, String> sc;

    @BeforeEach
    void setUp() {
        sc = new SegmentCompacter<>(versionController);
    }

    @AfterEach
    void tearDown() {
        sc = null;
    }

    @Test
    void test_basic_operations() {
        assertNotNull(sc);
    }

    @Test
    void prepareCompaction_snapshots_cache() {
        final List<Entry<Integer, String>> snapshot = List
                .of(Entry.of(1, "a"));
        when(segment.snapshotCacheEntries()).thenReturn(snapshot);
        when(segment.getId()).thenReturn(SegmentId.of(1));

        final List<Entry<Integer, String>> result = sc
                .prepareCompaction(segment);

        assertEquals(snapshot, result);
        verify(segment).resetSegmentIndexSearcher();
        verify(segment).freezeWriteCacheForFlush();
        verify(segment).snapshotCacheEntries();
    }

    @Test
    void publishCompaction_switches_active_version_and_bumps_version() {
        when(segment.getId()).thenReturn(SegmentId.of(1));
        final List<Entry<Integer, String>> snapshot = List
                .of(Entry.of(1, "a"), Entry.of(2, "b"));
        when(segment.freezeWriteCacheForFlush()).thenReturn(snapshot);
        when(segment.snapshotCacheEntries()).thenReturn(snapshot);
        when(files.getActiveVersion()).thenReturn(1L);
        when(segment.getSegmentFiles()).thenReturn(files);

        final SegmentCompacter.CompactionPlan<Integer, String> plan = sc
                .prepareCompactionPlan(segment);
        sc.publishCompaction(plan);

        verify(segment).switchActiveVersion(2L);
        verify(versionController).changeVersion();
    }
}
