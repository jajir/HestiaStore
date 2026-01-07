package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.junit.jupiter.api.Test;

class SegmentCoreTest {

    @Test
    void invalidateIteratorsBumpsVersion() {
        final VersionController versionController = new VersionController();
        final SegmentCore<Integer, String> core = createCore(versionController);
        try {
            final int before = versionController.getVersion();
            core.invalidateIterators();
            assertEquals(before + 1, versionController.getVersion());
        } finally {
            core.close();
        }
    }

    @Test
    void tryPutWithoutWaitingUpdatesWriteCacheCount() {
        final VersionController versionController = new VersionController();
        final SegmentCore<Integer, String> core = createCore(versionController);
        try {
            assertEquals(0, core.getNumberOfKeysInWriteCache());
            assertTrue(core.tryPutWithoutWaiting(1, "one"));
            assertEquals(1, core.getNumberOfKeysInWriteCache());
        } finally {
            core.close();
        }
    }

    private SegmentCore<Integer, String> createCore(
            final VersionController versionController) {
        final SegmentId segmentId = SegmentId.of(1);
        final var asyncDirectory = AsyncDirectoryAdapter
                .wrap(new MemDirectory());
        final SegmentFiles<Integer, String> files = new SegmentFiles<>(
                asyncDirectory, segmentId, new TypeDescriptorInteger(),
                new TypeDescriptorShortString(), 1024,
                List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()));
        final SegmentConf conf = new SegmentConf(5, 6, 10, 2, 1, 1024, 0.01D,
                1024, List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()));
        final SegmentPropertiesManager properties = new SegmentPropertiesManager(
                asyncDirectory, segmentId);
        final SegmentDataSupplier<Integer, String> dataSupplier = new SegmentDataSupplier<>(
                files, conf, properties);
        final SegmentResources<Integer, String> resources = new SegmentResourcesImpl<>(
                dataSupplier);
        final SegmentDeltaCacheController<Integer, String> deltaController = new SegmentDeltaCacheController<>(
                files, properties, resources,
                conf.getMaxNumberOfKeysInSegmentWriteCache(),
                conf.getMaxNumberOfKeysInChunk());
        final SegmentSearcher<Integer, String> searcher = new SegmentSearcher<>(
                files.getValueTypeDescriptor());
        return new SegmentCore<>(files, conf, versionController, properties,
                resources, deltaController, searcher);
    }
}
