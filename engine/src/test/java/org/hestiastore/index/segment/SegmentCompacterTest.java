package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;

import org.hestiastore.index.directory.FsDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
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

    @TempDir
    private File tempDir;

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

    @Test
    void cleanupOldVersion_ignores_missing_directory_listing() {
        final FsDirectory directory = new FsDirectory(tempDir);
        when(segment.getId()).thenReturn(SegmentId.of(1));
        when(files.getActiveVersion()).thenReturn(1L);
        when(files.getDirectory()).thenReturn(directory);
        when(segment.getSegmentFiles()).thenReturn(files);

        final SegmentCompacter.CompactionPlan<Integer, String> plan = sc
                .prepareCompactionPlan(segment);
        final Runnable cleanup = sc.buildCleanupTask(plan);
        assertTrue(tempDir.delete());

        assertDoesNotThrow(cleanup::run);
    }
}
