package org.hestiastore.index.segmentindex.wal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexWalConfiguration;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class WalSegmentCatalogTest {

    @Test
    void cleanupDeletesCheckpointedSealedSegments() {
        final IndexWalConfiguration wal = IndexWalConfiguration.builder().segmentSizeBytes(16L).build();
        final WalStorageMem storage = new WalStorageMem(new MemDirectory());
        final WalMetadataCatalog metadataCatalog = new WalMetadataCatalog(
                storage, LoggerFactory.getLogger(WalSegmentCatalogTest.class));
        final WalSegmentCatalog catalog = new WalSegmentCatalog(wal, storage,
                metadataCatalog,
                LoggerFactory.getLogger(WalSegmentCatalogTest.class));

        final WalSegmentDescriptor first = catalog.ensureActiveSegmentFor(1L,
                8);
        catalog.recordAppend(first, 8, 1L);
        final WalSegmentDescriptor second = catalog.ensureActiveSegmentFor(2L,
                12);
        catalog.recordAppend(second, 12, 2L);

        assertEquals(2, catalog.segmentCount());
        catalog.cleanupEligibleSegments(1L);

        assertEquals(1, catalog.segmentCount());
        assertEquals(second.name(), catalog.segments().get(0).name());
    }

    @Test
    void retentionPressureRequiresMoreThanActiveSegment() {
        final IndexWalConfiguration wal = IndexWalConfiguration.builder().segmentSizeBytes(64L)
                .maxBytesBeforeForcedCheckpoint(1L).build();
        final WalStorageMem storage = new WalStorageMem(new MemDirectory());
        final WalMetadataCatalog metadataCatalog = new WalMetadataCatalog(
                storage, LoggerFactory.getLogger(WalSegmentCatalogTest.class));
        final WalSegmentCatalog catalog = new WalSegmentCatalog(wal, storage,
                metadataCatalog,
                LoggerFactory.getLogger(WalSegmentCatalogTest.class));

        final WalSegmentDescriptor active = catalog.ensureActiveSegmentFor(1L,
                4);
        catalog.recordAppend(active, 4, 1L);

        assertFalse(catalog.isRetentionPressure());

        final WalSegmentDescriptor sealed = catalog.ensureActiveSegmentFor(2L,
                80);
        catalog.recordAppend(sealed, 80, 2L);

        assertTrue(catalog.isRetentionPressure());
    }
}
