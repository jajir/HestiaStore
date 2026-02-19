package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

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
    void prepareCompaction_freezes_without_snapshot_materialization() {
        when(segment.getId()).thenReturn(SegmentId.of(1));

        sc.prepareCompaction(segment);
        verify(segment).resetSegmentIndexSearcher();
        verify(segment).freezeWriteCacheForFlush();
        verify(segment, never()).snapshotCacheEntries();
    }

    @Test
    void publishCompaction_switches_active_version_and_bumps_version() {
        when(segment.getId()).thenReturn(SegmentId.of(1));
        when(files.getActiveVersion()).thenReturn(1L);
        when(segment.getSegmentFiles()).thenReturn(files);

        final SegmentCompacter.CompactionPlan<Integer, String> plan = sc
                .prepareCompactionPlan(segment);
        sc.publishCompaction(plan);

        verify(segment).switchActiveVersion(2L);
        verify(versionController).changeVersion();
    }
}
