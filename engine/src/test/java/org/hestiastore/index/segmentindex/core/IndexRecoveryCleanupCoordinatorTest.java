package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentDirectoryLayout;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;

@ExtendWith(MockitoExtension.class)
class IndexRecoveryCleanupCoordinatorTest {

    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;

    private Directory directory;
    private KeyToSegmentMap<Integer> keyToSegmentMap;
    private KeyToSegmentMapSynchronizedAdapter<Integer> synchronizedKeyToSegmentMap;
    private IndexRecoveryCleanupCoordinator<Integer, String> coordinator;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        keyToSegmentMap = new KeyToSegmentMap<>(directory,
                new TypeDescriptorInteger());
        synchronizedKeyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                keyToSegmentMap);
        coordinator = new IndexRecoveryCleanupCoordinator<>(
                LoggerFactory.getLogger(
                        IndexRecoveryCleanupCoordinatorTest.class),
                directory, synchronizedKeyToSegmentMap, segmentRegistry,
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
    void cleanupOrphanedSegmentDirectories_deletesOnlyUnmappedNonBootstrapDirectories() {
        keyToSegmentMap.insertKeyToSegment(1);
        directory.mkdir("segment-00000");
        directory.mkdir("segment-00003");
        when(segmentRegistry.deleteSegment(SegmentId.of(3)))
                .thenReturn(SegmentRegistryResult.ok(null));

        coordinator.cleanupOrphanedSegmentDirectories();

        verify(segmentRegistry).deleteSegment(SegmentId.of(3));
        verify(segmentRegistry, never()).deleteSegment(SegmentId.of(0));
        verify(segmentRegistry, never()).deleteSegment(SegmentId.of(1));
    }

    @Test
    void hasSegmentLockFile_detectsExistingLockFileInsideSegmentDirectory() {
        final SegmentId segmentId = SegmentId.of(3);
        final Directory segmentDirectory = directory
                .openSubDirectory(segmentId.getName());
        final String lockFileName = new SegmentDirectoryLayout(segmentId)
                .getLockFileName();
        segmentDirectory.touch(lockFileName);

        assertTrue(coordinator.hasSegmentLockFile(segmentId));
        assertFalse(coordinator.hasSegmentLockFile(SegmentId.of(4)));
    }
}
