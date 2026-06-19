package org.hestiastore.index.segmentindex.core.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentDirectoryLayout;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.hestiastore.index.segmentindex.routemap.PersistentSegmentRouteMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SegmentDirectoryRecoveryScannerTest {

    private Directory directory;
    private SegmentRouteMap<Integer> synchronizedKeyToSegmentMap;
    private SegmentDirectoryRecoveryScanner<Integer> inspector;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        synchronizedKeyToSegmentMap = new PersistentSegmentRouteMap<>(directory,
                new TypeDescriptorInteger());
        inspector = new SegmentDirectoryRecoveryScanner<>(directory,
                synchronizedKeyToSegmentMap);
    }

    @AfterEach
    void tearDown() {
        if (synchronizedKeyToSegmentMap != null
                && !synchronizedKeyToSegmentMap.wasClosed()) {
            synchronizedKeyToSegmentMap.close();
        }
    }

    @Test
    void discoverOrphanedSegmentDirectories_returnsOnlyUnmappedNonBootstrapSegments() {
        synchronizedKeyToSegmentMap.extendMaxKeyIfNeeded(1);
        directory.mkdir("segment-00000");
        directory.mkdir("segment-00003");
        directory.mkdir("segment-00004");
        directory.touch("notes.txt");

        final List<SegmentId> orphanedSegments = inspector.discoverOrphanedSegmentDirectories();

        assertEquals(List.of(SegmentId.of(3), SegmentId.of(4)),
                orphanedSegments);
    }

    @Test
    void hasSegmentLockFile_detectsExistingLockFileInsideSegmentDirectory() {
        final SegmentId segmentId = SegmentId.of(3);
        final Directory segmentDirectory = directory
                .openSubDirectory(segmentId.getName());
        final String lockFileName = new SegmentDirectoryLayout(segmentId)
                .getLockFileName();
        segmentDirectory.touch(lockFileName);

        assertTrue(inspector.hasSegmentLockFile(segmentId));
        assertFalse(inspector.hasSegmentLockFile(SegmentId.of(4)));
    }
}
